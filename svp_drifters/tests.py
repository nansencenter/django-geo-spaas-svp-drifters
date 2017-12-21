from django.test import TestCase, override_settings
from django.contrib.gis.geos import LineString

from svp_drifters.models import SVPDrifter
from geospaas.vocabularies.models import Platform, Instrument, DataCenter, ISOTopicCategory
from geospaas.catalog.models import GeographicLocation, DatasetURI, Source, Dataset

import datetime
from math import ceil
import tempfile
import warnings
import calendar
import os


class SVPDrifterModelTest(TestCase):
    fixtures = ['vocabularies']
    TEST_DIR = '/vagrant/shared/src/django-geo-spaas-svp-drifters/test_data/'
    METADATA_PATH = os.path.join(TEST_DIR, 'dirfl_1_5000_test.dat')
    DATA_PATH = os.path.join(TEST_DIR, 'buoydata_1_5000_test.dat')
    TMP = tempfile.mkdtemp()

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

    @override_settings(PRODUCTS_ROOT=TMP)
    def test_get_or_create_several_fls(self):
        cnt = SVPDrifter.objects.get_or_create(self.METADATA_PATH, self.DATA_PATH)
        self.assertEqual(cnt, 3)
        dataset = Dataset.objects.filter(source__platform__short_name='BUOYS')
        self.assertGreater(len(dataset), 0)
        data_uris = set(el.dataseturi_set.all()[1].uri for el in dataset)
        self.assertEqual(len(data_uris), cnt)
        metadata_uris = set(el.dataseturi_set.all()[0].uri for el in dataset)
        self.assertEqual(len(metadata_uris), 1)
        self.assertTrue(os.path.exists(metadata_uris.pop()))
        map(lambda uri: self.assertTrue(os.path.exists(uri)), data_uris)
        # All subset in the db has to get 2 uris
        map(lambda el: self.assertEqual(len(el.dataseturi_set.all()), 2), dataset)

    @override_settings(PRODUCTS_ROOT=TMP)
    def test_get_or_create_data_chunk(self):
        start_date = datetime.datetime(1988, 3, 8, 0, 0, 0, 0)
        end_date = datetime.datetime(1988, 9, 13, 18, 0, 0, 0)
        chunk_num = ceil((end_date - start_date).days / 5.)

        with warnings.catch_warnings(record=True) as w:
            cnt = SVPDrifter.objects.get_or_create(
                self.METADATA_PATH, os.path.join(self.TEST_DIR, 'buoydata_1_test.dat'))

            self.assertEqual(len(w), 1)
            self.assertTrue('Not all buoys were added to the database!' in str(w[-1].message))

        dataset = Dataset.objects.filter(source__platform__short_name='BUOYS').order_by('time_coverage_start')
        self.assertEqual(len(dataset), chunk_num)
        # TODO: Compare two datetime objects
        first_subset = dataset.first()
        self.assertEqual(first_subset.time_coverage_start.year, start_date.year)
        self.assertEqual(first_subset.time_coverage_start.month, start_date.month)
        self.assertEqual(first_subset.time_coverage_start.day, start_date.day)
        self.assertEqual(first_subset.time_coverage_start.hour, start_date.hour)
        last_subset = dataset.last()
        print(last_subset.time_coverage_end)
        self.assertEqual(last_subset.time_coverage_end.year, end_date.year)
        self.assertEqual(last_subset.time_coverage_end.month, end_date.month)
        self.assertEqual(last_subset.time_coverage_end.day, end_date.day)
        self.assertEqual(last_subset.time_coverage_end.hour, end_date.hour)
