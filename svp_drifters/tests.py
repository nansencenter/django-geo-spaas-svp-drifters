from django.test import TestCase
from django.contrib.gis.geos import LineString

from svp_drifters.models import SVPDrifter
from geospaas.vocabularies.models import Platform, Instrument, DataCenter, ISOTopicCategory
from geospaas.catalog.models import GeographicLocation, DatasetURI, Source, Dataset

import datetime
import calendar
import os


class SVPDrifterModelTest(TestCase):
    fixtures = ['vocabularies']
    METADATA_PATH = '/vagrant/shared/src/django-geo-spaas-svp-drifters/test_data/dirfl_1_5000_test.dat'

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

    def test_convert_datetime(self):
        months = [str(m) for m in xrange(1, 13)]
        year = '2010'
        for month in months:
            # Get number of days in the month
            daynum = calendar.monthrange(int(year), int(month))[1]
            # Simulate real daytime values: 1.0, 1.25, 1.5, 1.75, 2.0, 2.25 .....
            daytimes = [str(d) for d in map(lambda x: x / 100.0, range(100, (daynum + 1) * 100, 25))]
            # Create an array with day number in formt: 1 1 1 1 2 2 2 2 3 3 3 3 ...
            days = []
            map(lambda x: days.extend([1 * x] * 4), range(1, daynum + 1))
            # Create an array with day number in formt: 1 1 1 1 2 2 2 2 3 3 3 3 ...
            hours = range(0, 24, 6) * daynum
            for i in xrange(len(hours)):
                converted_date = SVPDrifter.objects.convert_datetime(month, daytimes[i], year)
                manual_date = datetime.datetime(year=int(year), month=int(month), day=days[i], hour=hours[i])
                self.assertEqual(converted_date, manual_date)

    def test_read_metadata(self):
        metadata = SVPDrifter.objects.read_metadata(self.METADATA_PATH)
        self.assertIsInstance(metadata, list)
        self.assertEqual(len(metadata), 3)
        self.assertEqual(len(metadata[1]), 15)

    def test_gen_file_name(self):
        test_file_name = 'SVP_7702986_198803070000_198809132355.csv'
        metadata = SVPDrifter.objects.read_metadata(self.METADATA_PATH)
        file_name = SVPDrifter.objects.gen_file_name(metadata[0])
        self.assertIsInstance(file_name, str)
        self.assertEqual(file_name, test_file_name)

    def test_get_geometry(self):
        lats = range(0, 12, 2)
        lons_1 = range(10, 16, 1)
        geo_1 = LineString(*zip(lons_1, lats))
        svp_geo_1 = SVPDrifter.objects.get_geometry(lons_1, lats)
        self.assertEqual(geo_1, svp_geo_1)
        self.assertIsInstance(svp_geo_1, LineString)
        lons_2_180 = range(-110, -104, 1)
        lons_2_360 = range(250, 256, 1)
        geo_2 = LineString(*zip(lons_2_180, lats))
        svp_geo_2 = SVPDrifter.objects.get_geometry(lons_2_360, lats)
        self.assertEqual(geo_2, svp_geo_2)

