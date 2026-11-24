"""MEDRE monitoring fields survive the coordinator's poll cadence."""
import json
import runpy
import time
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
import unittest

from test_runtime_monitoring import Base, extracted

ROOT = Path(__file__).resolve().parents[1]
COMPONENT = ROOT / 'custom_components/pymc_repeater'
FIELDS = ('phase', 'health_scope', 'runtime_health', 'health_observed_at',
          'adapters_started', 'startup_routes_registered', 'startup_routes_blocked')
CADENCES = (15, 30, 60, 120, 300)


def namespace():
    helpers = runpy.run_path(str(COMPONENT / 'monitoring.py'))
    def nested(data, *keys):
        for key in keys:
            data = data.get(key) if isinstance(data, dict) else None
        return data
    ns = dict(helpers, json=json, PyMCBaseEntity=Base, SensorEntity=type('Sensor', (), {}),
        BinarySensorEntity=type('Binary', (), {}), EntityCategory=SimpleNamespace(DIAGNOSTIC='diagnostic'),
        SensorDeviceClass=SimpleNamespace(DURATION='duration', TIMESTAMP='timestamp'),
        SensorStateClass=SimpleNamespace(MEASUREMENT='measurement'),
        BinarySensorDeviceClass=SimpleNamespace(PROBLEM='problem', CONNECTIVITY='connectivity'),
        DeviceInfo=dict, DOMAIN='pymc_repeater', MANUFACTURER='openHop', _nested=nested,
        _external_sensor_readings=lambda data: nested(data, 'stats', 'sensors', 'readings') or [],
        _external_sensor_identity=lambda reading: reading['name'])
    return extracted('monitoring_entities.py', ns)


class MedreMonitoringCadenceTests(unittest.TestCase):
    def coordinator(self, cadence, age):
        stamp = time.time() - age
        coordinator = SimpleNamespace(data={}, last_update_success=True,
            update_interval=timedelta(seconds=cadence))
        coordinator.data = {'plugin_summary': {'plugins': [
                {'id': 'medre.bridge', 'enabled': True, 'state': 'RUNNING', 'version': '0.1.0'}],
            'medre_status': {'updated_at': stamp, 'phase': 'running',
                             'health_scope': 'retained', 'runtime_health': 'healthy',
                             'health_observed_at': stamp - 1, 'adapters_started': 2,
                             'startup_routes_registered': 4, 'startup_routes_blocked': 1}}}
        return coordinator

    def test_medre_fields_survive_the_poll_cycle_at_every_cadence(self):
        for cadence in CADENCES:
            with self.subTest(cadence=cadence):
                ns = namespace()
                # Worst case between polls: the source heartbeat was already at
                # its own ceiling when fetched and one poll cycle has passed.
                coordinator = self.coordinator(cadence, cadence + 30)
                values = ns['_snapshot'](coordinator, False)
                for field in FIELDS:
                    self.assertIn(('plugin', 'medre.bridge', 'medre_' + field), values)
                entry = SimpleNamespace(unique_id='entry', entry_id='fallback')
                for field in FIELDS:
                    entity = ns['MonitoringSensor'](entry, coordinator,
                        ('plugin', 'medre.bridge', 'medre_' + field))
                    self.assertTrue(entity.available, field)

    def test_genuinely_stale_heartbeat_is_unavailable_at_every_cadence(self):
        for cadence in CADENCES:
            with self.subTest(cadence=cadence):
                ns = namespace()
                limit = max(60, cadence * 3)
                coordinator = self.coordinator(cadence, limit + 1)
                values = ns['_snapshot'](coordinator, False)
                for field in FIELDS:
                    self.assertNotIn(('plugin', 'medre.bridge', 'medre_' + field), values)
                entry = SimpleNamespace(unique_id='entry', entry_id='fallback')
                entity = ns['MonitoringSensor'](entry, coordinator,
                    ('plugin', 'medre.bridge', 'medre_phase'))
                self.assertFalse(entity.available)

    def test_unknown_cadence_keeps_the_source_heartbeat_ceiling(self):
        ns = namespace()
        coordinator = self.coordinator(15, 31)
        coordinator.update_interval = None
        values = ns['_snapshot'](coordinator, False)
        self.assertNotIn(('plugin', 'medre.bridge', 'medre_phase'), values)
