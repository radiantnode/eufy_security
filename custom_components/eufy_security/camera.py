from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import traceback

import aiohttp

from haffmpeg.camera import CameraMjpeg
from haffmpeg.tools import ImageFrame
from base64 import b64decode
from homeassistant.components import ffmpeg
from homeassistant.components.camera import Camera, CameraEntityFeature
from homeassistant.components.ffmpeg import DATA_FFMPEG
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, SupportsResponse
from homeassistant.helpers import entity_platform
from homeassistant.helpers.aiohttp_client import async_aiohttp_proxy_stream, async_get_clientsession
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import COORDINATOR, DOMAIN, Schema
from .coordinator import EufySecurityDataUpdateCoordinator
from .entity import EufySecurityEntity
from .eufy_security_api.camera import (
    STREAM_SLEEP_SECONDS,
    STREAM_TIMEOUT_SECONDS,
    StreamProvider,
    StreamStatus,
)
from .eufy_security_api import frames as wake_frames
from .eufy_security_api.const import GO2RTC_API_PORT
from .eufy_security_api.metadata import Metadata
from .eufy_security_api.util import wait_for_value_to_equal

_LOGGER: logging.Logger = logging.getLogger(__package__)

#: Hubble fork: one stream start at a time. The HomeBase streams one camera at
#: a time, and a second start cuts the first off and confuses
#: eufy-security-ws's bookkeeping, so the sampler holds this from start to
#: stop and a start asked for by a service waits on it. One HomeBase here.
_STATION_LOCK = asyncio.Lock()


async def async_setup_entry(hass: HomeAssistant, config_entry: ConfigEntry, async_add_entities: AddEntitiesCallback) -> None:
    """Setup camera entities."""
    coordinator: EufySecurityDataUpdateCoordinator = hass.data[DOMAIN][COORDINATOR]
    product_properties = []
    for product in coordinator.devices.values():
        if product.is_camera is True:
            product_properties.append(Metadata.parse(product, {"name": "camera", "label": "Camera"}))

    entities = [EufySecurityCamera(coordinator, metadata) for metadata in product_properties]
    async_add_entities(entities)

    # Hubble fork: wake each camera about once a minute for a frame, so the
    # last twenty minutes can be looked back through. See frames.py.
    sampler = hass.async_create_background_task(_sample_forever(hass, entities), "eufy_security sampler")
    config_entry.async_on_unload(sampler.cancel)

    # register entity level services
    platform = entity_platform.async_get_current_platform()
    platform.async_register_entity_service("generate_image", {}, "_generate_image")
    platform.async_register_entity_service("start_p2p_livestream", {}, "_start_livestream")
    platform.async_register_entity_service("stop_p2p_livestream", {}, "_stop_livestream")
    platform.async_register_entity_service("start_rtsp_livestream", {}, "_start_rtsp_livestream")
    platform.async_register_entity_service("stop_rtsp_livestream", {}, "_stop_rtsp_livestream")
    platform.async_register_entity_service("ptz", Schema.PTZ_SERVICE_SCHEMA.value, "_async_ptz")
    platform.async_register_entity_service("ptz_up", {}, "_async_ptz_up")
    platform.async_register_entity_service("ptz_down", {}, "_async_ptz_down")
    platform.async_register_entity_service("ptz_left", {}, "_async_ptz_left")
    platform.async_register_entity_service("ptz_right", {}, "_async_ptz_right")
    platform.async_register_entity_service("ptz_360", {}, "_async_ptz_360")
    platform.async_register_entity_service("preset_position", Schema.PRESET_POSITION_SERVICE_SCHEMA.value, "_async_preset_position")
    platform.async_register_entity_service("save_preset_position", Schema.PRESET_POSITION_SERVICE_SCHEMA.value, "_async_save_preset_position")
    platform.async_register_entity_service("delete_preset_position", Schema.PRESET_POSITION_SERVICE_SCHEMA.value, "_async_delete_preset_position")
    platform.async_register_entity_service("calibrate", {}, "_async_calibrate")

    platform.async_register_entity_service("trigger_camera_alarm_with_duration", Schema.TRIGGER_ALARM_SERVICE_SCHEMA.value, "_async_alarm_trigger")
    platform.async_register_entity_service("reset_alarm", {}, "_async_reset_alarm")
    platform.async_register_entity_service("quick_response", Schema.QUICK_RESPONSE_SERVICE_SCHEMA.value, "_async_quick_response")
    platform.async_register_entity_service("snooze", Schema.SNOOZE.value, "_snooze")
    # Hubble fork: the frames kept from each wake. See eufy_security_api/frames.py.
    platform.async_register_entity_service(
        "get_frames", Schema.GET_FRAMES_SERVICE_SCHEMA.value, "_get_frames", supports_response=SupportsResponse.ONLY
    )


async def _sample_forever(hass: HomeAssistant, cameras: list) -> None:
    """Hubble fork: whichever camera has gone longest without a wake, once
    it's been SAMPLE_EVERY_S, gets one, a frame kept. Never while any camera
    is streaming: that's someone looking, and it counts as a wake anyway."""
    root = hass.config.path()
    last: dict[str, float] = {}
    for camera in cameras:
        serial = str(camera.product.serial_no)
        newest = await hass.async_add_executor_job(wake_frames.wakes, root, serial)
        last[serial] = wake_frames.wake_time(newest[0]) if newest else 0.0
    while True:
        await asyncio.sleep(wake_frames.SAMPLER_TICK_S)
        try:
            for camera in cameras:
                serial = str(camera.product.serial_no)
                newest = await hass.async_add_executor_job(wake_frames.wakes, root, serial)
                if newest:
                    last[serial] = max(last.get(serial, 0.0), wake_frames.wake_time(newest[0]))
            due = min(cameras, key=lambda c: last.get(str(c.product.serial_no), 0.0))
            if time.time() - last.get(str(due.product.serial_no), 0.0) < wake_frames.SAMPLE_EVERY_S:
                continue
            async with _STATION_LOCK:
                if any(c.product.stream_status != StreamStatus.IDLE for c in cameras):
                    continue
                if await due._sample():
                    last[str(due.product.serial_no)] = time.time()
                await asyncio.sleep(wake_frames.COOLDOWN_S)
        except asyncio.CancelledError:
            raise
        except Exception:  # pylint: disable=broad-except
            _LOGGER.warning("sampler - a sample failed", exc_info=True)


class EufySecurityCamera(Camera, EufySecurityEntity):
    """Base camera entity for integration"""

    def __init__(self, coordinator: EufySecurityDataUpdateCoordinator, metadata: Metadata) -> None:
        Camera.__init__(self)
        EufySecurityEntity.__init__(self, coordinator, metadata)
        self._attr_supported_features = CameraEntityFeature.STREAM
        self._attr_name = f"{self.product.name}"

        # camera image
        self._last_url = None
        self._last_image = None
        if self.product.picture_base64 is not None:
            self._last_image = self.product.picture_bytes

        # ffmpeg entities
        self.ffmpeg = self.coordinator.hass.data[DATA_FFMPEG]

        # Hubble fork: the burst being taken from this wake, if one is.
        self._burst: asyncio.Task | None = None

    async def stream_source(self) -> str:
        if self.is_streaming is False:
            return None
        return self.product.stream_url

    async def handle_async_mjpeg_stream(self, request):
        """this is probabaly triggered by user request, turn on"""
        stream_source = await self.stream_source()
        if stream_source is None:
            return await super().handle_async_mjpeg_stream(request)
        stream = CameraMjpeg(self.ffmpeg.binary)
        await stream.open_camera(stream_source)
        try:
            return await async_aiohttp_proxy_stream(
                self.hass,
                request,
                await stream.get_reader(),
                self.ffmpeg.ffmpeg_stream_content_type,
            )
        finally:
            await stream.close()

    async def async_create_stream(self):
        if self.coordinator.config.no_stream_in_hass is True:
            return None
        return await super().async_create_stream()

    async def _start_hass_streaming(self):
        await wait_for_value_to_equal(self.product.__dict__, "stream_status", StreamStatus.STREAMING)
        await self._stop_hass_streaming()
        await self.async_create_stream()
        if self.stream is not None:
            await self.stream.start()
        # Hubble patch 2026-10-04: no picture grabbed here. It made every
        # start_p2p_livestream wait ~3.5s on ffmpeg and a keyframe before
        # returning, and whoever started the stream asks for a picture next
        # anyway. Was: await self.async_camera_image()

    async def _stop_hass_streaming(self):
        if self.stream is not None:
            await self.stream.stop()
            self.stream = None

    @property
    def is_streaming(self) -> bool:
        """Return true if the device is recording."""
        return self.product.stream_status == StreamStatus.STREAMING

    @property
    def available(self) -> bool:
        return True

    @property
    def extra_state_attributes(self):
        return {"stream_debug": self.product.stream_debug}

    async def _get_image_from_stream_url(self, width, height):
        while True:
            result = await ffmpeg.async_get_image(self.hass, await self.stream_source(), width=width, height=height)
            if result is not None:
                _LOGGER.debug(f"_get_image_from_stream_url - received {len(result)}")
                return result
            _LOGGER.debug(f"_get_image_from_stream_url - is_empty {result is None}")
            await asyncio.sleep(STREAM_SLEEP_SECONDS)

    async def _get_image_from_go2rtc(self) -> bytes | None:
        """Hubble patch 2026-10-04: a P2P stream is already decoded by go2rtc,
        which hands out a frame in 1-2s; ffmpeg reopening the RTSP copy took
        3-4s every time. None when it can't, and the caller falls back."""
        if self.product.stream_provider != StreamProvider.P2P:
            return None
        url = f"http://{self.coordinator.config.rtsp_server_address}:{GO2RTC_API_PORT}/api/frame.jpeg"
        try:
            async with async_get_clientsession(self.hass).get(
                url, params={"src": str(self.product.serial_no)}, timeout=STREAM_TIMEOUT_SECONDS
            ) as response:
                if response.status == 200 and response.content_type == "image/jpeg":
                    return await response.read()
        except Exception as ex:  # pylint: disable=broad-except
            _LOGGER.debug(f"_get_image_from_go2rtc - {ex}")
        return None

    async def async_camera_image(self, width: int | None = None, height: int | None = None) -> bytes | None:
        _LOGGER.debug(f"image 1 - {self.is_streaming} - {self.stream}")
        if self.is_streaming is True:
            if (image := await self._get_image_from_go2rtc()) is not None:
                self._last_image = image
                return image
            with contextlib.suppress(asyncio.TimeoutError):
                self._last_image = await asyncio.wait_for(self._get_image_from_stream_url(width, height), STREAM_TIMEOUT_SECONDS)
            _LOGGER.debug(f"image 2 - is_empty {self._last_image is None}")

        _LOGGER.debug(f"async_camera_image 5 - is_empty {self._last_image is None}")
        if self._last_image is not None:
            _LOGGER.debug(f"async_camera_image 6 - {len(self._last_image)}")
        return self._last_image

    async def _sample(self) -> bool:
        """Hubble fork: wake this camera for one frame and keep it. Its P2P
        stream started without Home Assistant's own stream worker, the frame
        taken from go2rtc, and the stream always stopped again."""
        woken = time.time()
        if await self.product.start_livestream() is False:
            with contextlib.suppress(Exception):
                await self.product.stop_livestream()
            return False
        image = None
        try:
            deadline = time.monotonic() + 10
            while image is None and time.monotonic() < deadline:
                image = await self._get_image_from_go2rtc()
                if image is None:
                    await asyncio.sleep(0.5)
        finally:
            with contextlib.suppress(Exception):
                await self.product.stop_livestream()
        if image is None:
            return False
        await self.hass.async_add_executor_job(
            wake_frames.save_wake, self.hass.config.path(), str(self.product.serial_no), woken, [(0.0, image)]
        )
        return True

    async def _start_livestream(self) -> None:
        """start byte based livestream on camera"""
        # Hubble fork: after any sample in progress, not over it.
        async with _STATION_LOCK:
            started = await self.product.start_livestream()
        if started is False:
            await self._stop_livestream()
        else:
            await self._start_hass_streaming()
            # Hubble fork: keep a burst from every wake, whoever woke it.
            if self._burst is None or self._burst.done():
                self._burst = self.hass.async_create_background_task(
                    self._capture_burst(time.time()), f"eufy_security burst {self.product.serial_no}"
                )
        self.async_write_ha_state()

    async def _capture_burst(self, woken: float) -> None:
        """Hubble fork: a few frames from go2rtc's MJPEG of this stream,
        BURST_GAP_S apart, saved with when the camera was woken. Ends early if
        the stream stops."""
        api = f"http://{self.coordinator.config.rtsp_server_address}:{GO2RTC_API_PORT}/api"
        serial = str(self.product.serial_no)
        burst = f"{serial}_burst"
        # go2rtc only serves MJPEG from a source that makes it, so a second
        # stream transcodes this one; ffmpeg runs only while it's being read.
        # go2rtc keeps an API-made stream in memory and answers 400 when it
        # can't also write it to its config file, as for the integration's own.
        with contextlib.suppress(asyncio.TimeoutError, aiohttp.ClientError):
            async with async_get_clientsession(self.hass).put(
                f"{api}/streams", params={"name": burst, "src": f"ffmpeg:{serial}#video=mjpeg"}, timeout=5
            ):
                pass
        url = f"{api}/stream.mjpeg"
        taken: list[tuple[float, bytes]] = []
        first = None
        deadline = time.monotonic() + wake_frames.BURST_TIMEOUT_S
        # The stream is up before its video reaches go2rtc, and go2rtc answers
        # a source with no media yet with an empty stream that ends at once.
        # So ask again until frames come, or the time's up, or it stopped.
        while len(taken) < wake_frames.BURST_FRAMES and time.monotonic() < deadline and self.is_streaming:
            try:
                async with async_get_clientsession(self.hass).get(
                    url,
                    params={"src": burst},
                    timeout=aiohttp.ClientTimeout(total=max(1.0, deadline - time.monotonic())),
                ) as response:
                    buffer = b""
                    async for chunk in response.content.iter_any():
                        buffer += chunk
                        while True:
                            frame, buffer = wake_frames.next_jpeg(buffer)
                            if frame is None:
                                break
                            now = time.monotonic()
                            if first is None or now - first >= len(taken) * wake_frames.BURST_GAP_S:
                                first = first if first is not None else now
                                taken.append((round(now - first, 2), frame))
                        if len(taken) >= wake_frames.BURST_FRAMES:
                            break
            except (asyncio.TimeoutError, aiohttp.ClientError) as ex:
                _LOGGER.debug(f"_capture_burst - {ex}")
            if len(taken) < wake_frames.BURST_FRAMES:
                await asyncio.sleep(0.25)
        if taken:
            await self.hass.async_add_executor_job(
                wake_frames.save_wake, self.hass.config.path(), str(self.product.serial_no), woken, taken
            )
        _LOGGER.debug(f"_capture_burst - kept {len(taken)} frames")

    async def _get_frames(self, wakes: int = 1, frames: int = 0, width: int = 960, wait: bool = True,
                          minutes: int = 0) -> dict:
        """Hubble fork: the frames kept from the last `wakes` wakes, or from
        every wake in the last `minutes`."""
        if wait and self._burst is not None and not self._burst.done():
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(self._burst), wake_frames.BURST_TIMEOUT_S)
        kept = await self.hass.async_add_executor_job(
            wake_frames.load_wakes, self.hass.config.path(), str(self.product.serial_no), wakes, width, frames or None,
            time.time() - minutes * 60 if minutes else None,
        )
        return {"wakes": kept}

    async def _stop_livestream(self) -> None:
        """stop byte based livestream on camera"""
        await self._stop_hass_streaming()
        await self.product.stop_livestream()
        self.async_write_ha_state()

    async def _start_rtsp_livestream(self) -> None:
        """start rtsp based livestream on camera"""
        if await self.product.start_rtsp_livestream() is False:
            await self._stop_rtsp_livestream()
        else:
            await self._start_hass_streaming()
        self.async_write_ha_state()

    async def _stop_rtsp_livestream(self) -> None:
        """stop rtsp based livestream on camera"""
        await self._stop_hass_streaming()
        await self.product.stop_rtsp_livestream()
        self.async_write_ha_state()

    async def _async_alarm_trigger(self, duration: int = 10):
        """trigger alarm for a duration on camera"""
        await self.product.trigger_alarm(duration)

    async def _async_reset_alarm(self) -> None:
        """reset ongoing alarm"""
        await self.product.reset_alarm()

    async def async_turn_on(self) -> None:
        """Turn off camera."""
        if self.product.stream_provider == StreamProvider.RTSP:
            await self._start_rtsp_livestream()
        else:
            await self._start_livestream()

    async def async_turn_off(self) -> None:
        """Turn off camera."""
        if self.product.stream_provider == StreamProvider.RTSP:
            await self._stop_rtsp_livestream()
        else:
            await self._stop_livestream()

    async def _async_ptz(self, direction: str) -> None:
        await self.product.ptz(direction)

    async def _async_ptz_up(self) -> None:
        await self.product.ptz_up()

    async def _async_ptz_down(self) -> None:
        await self.product.ptz_down()

    async def _async_ptz_left(self) -> None:
        await self.product.ptz_left()

    async def _async_ptz_right(self) -> None:
        await self.product.ptz_right()

    async def _async_ptz_360(self) -> None:
        await self.product.ptz_360()

    async def _async_preset_position(self, position: int) -> None:
        await self.product.preset_position(position)

    async def _async_save_preset_position(self, position: int) -> None:
        await self.product.save_preset_position(position)

    async def _async_delete_preset_position(self, position: int) -> None:
        await self.product.delete_preset_position(position)

    async def _async_calibrate(self) -> None:
        await self.product.calibrate()

    async def _generate_image(self) -> None:
        await self.async_camera_image()

    async def _async_quick_response(self, voice_id: int) -> None:
        await self.product.quick_response(voice_id)

    async def _snooze(self, snooze_time: int, snooze_chime: bool, snooze_motion: bool, snooze_homebase: bool) -> None:
        await self.product.snooze(snooze_time, snooze_chime, snooze_motion, snooze_homebase)
