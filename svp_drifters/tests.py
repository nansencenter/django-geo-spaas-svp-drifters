from django.test import TestCase
from svp_drifters.models import SVPDrifter
from geospaas.vocabularies.models import Platform, Instrument, DataCenter, ISOTopicCategory
from geospaas.catalog.models import GeographicLocation, DatasetURI, Source, Dataset


class SVPDrifterModelTest(TestCase):
    fixtures = ['vocabularies']

    def test_set_metadata(self):
        src, dc, iso = SVPDrifter.objects.set_metadata()

        self.assertIsInstance(src, Source)
        self.assertEqual(src.platform.short_name, 'BUOYS')
        self.assertEqual(src.instrument.short_name, 'DRIFTING BUOYS')

        self.assertIsInstance(dc, DataCenter)
        self.assertEqual(dc.short_name, 'DOC/NOAA/OAR/AOML')

        self.assertIsInstance(iso, ISOTopicCategory)
        self.assertEqual(iso.name, 'Oceans')

    def test_shift_longitude(self):
        lons360 = range(0, 360, 10)
        lons180 = range(0, 180, 10) + range(-180, 0, 10)

        shifted_lons360 = [SVPDrifter.objects.shift_longitude(lon) for lon in lons360]
        self.assertEqual(lons180, shifted_lons360)
        self.assertIsInstance(shifted_lons360[0], float)
