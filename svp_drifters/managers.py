import numpy as np
import warnings
import datetime
import os
import pythesint as pti

from django.db import models
from django.conf import settings
from django.contrib.gis.geos import LineString

from geospaas.utils import validate_uri, nansat_filename

from geospaas.vocabularies.models import Platform, Instrument, DataCenter, ISOTopicCategory
from geospaas.catalog.models import GeographicLocation, DatasetURI, Source, Dataset


class SVPDrifterManager(models.Manager):

    CHUNK_DURATION = 5
    COL_NAMES = ['id', 'month', 'daytime', 'year', 'lat', 'lon', 't',
                 've', 'vn', 'speed', 'varlat', 'varlon', 'vart']

    def set_metadata(self):
        pp = Platform.objects.get(short_name='BUOYS')
        ii = Instrument.objects.get(short_name='DRIFTING BUOYS')
        src = Source.objects.get_or_create(platform=pp, instrument=ii)[0]
        dc = DataCenter.objects.get(short_name='DOC/NOAA/OAR/AOML')
        iso = ISOTopicCategory.objects.get(name='Oceans')
        return src, dc, iso

    def shift_longitude(self, lon):
        return np.mod(float(lon) + 180, 360) - 180.

    def convert_datetime(self, month, daytime, year):
        daytime = float(daytime)
        day = int(daytime // 1)
        time = int((daytime % 1) / (1. / 24))
        return datetime.datetime(year=int(year), month=int(month), day=day, hour=time)

    def gen_file_name(self, metadata):
        time_start = ''.join(metadata[4:6]).replace('/', '').replace(':', '')
        time_end = ''.join(metadata[8:10]).replace('/', '').replace(':', '')
        file_name = '_'.join([metadata[3], metadata[0], time_start, time_end]) + '.csv'
        return file_name

    def read_metadata(self, path):
        arr = []
        with open(path, 'r') as meta_file:
            for line in meta_file:
                arr.append(line.strip().split())
        return arr

    def get_geometry(self, lons, lats):
        lon_lat = zip([self.shift_longitude(float(x)) for x in lons],
                      [float(x) for x in lats])
        geometry = LineString(lon_lat)
        return geometry

    def split_time_coverage(self, start, end):
        period = (end - start).days
        steps = range(0, period, self.CHUNK_DURATION)
        # TODO: Don't catch datasets smaller then one day
        time_steps = [start + datetime.timedelta(days=step) for step in steps]
        if time_steps[-1] != end:
            time_steps.append(end)
        return time_steps

    def process_data(self, buoy_metadata, data, iso, data_center, source, metadata_path):
        convert_datetime_vctrz = np.vectorize(self.convert_datetime)
        data = np.array(data)
        # Export buoy data to csv and get path to file
        export_path = self.export(settings.PRODUCTS_ROOT, buoy_metadata, data)
        # Create timestamp from row data
        timestamp = convert_datetime_vctrz(data[:, 1], data[:, 2], data[:, 3])
        # Separate whole buoy dataset for several intervals with <chunk_duration> step
        time_steps = self.split_time_coverage(timestamp.min(), timestamp.max())
        # Start and end datetime for subset
        for step in xrange(len(time_steps) - 1):
            subset = data[(timestamp >= time_steps[step]) & (timestamp <= time_steps[step + 1])]
            geometry = self.get_geometry(lons=subset[:, 5], lats=subset[:, 4])
            geoloc, geo_cr = GeographicLocation.objects.get_or_create(geometry=geometry)

            ds, ds_cr = Dataset.objects.get_or_create(
                entry_title='%s drifter no. %s' % (buoy_metadata[3], buoy_metadata[0]),
                ISO_topic_category=iso,
                data_center=data_center,
                summary='',
                time_coverage_start=time_steps[step],
                time_coverage_end=time_steps[step + 1],
                source=source,
                geographic_location=geoloc)
            if ds_cr:
                meta_uri, muc = DatasetURI.objects.get_or_create(uri=metadata_path, dataset=ds)
                data_uri, duc = DatasetURI.objects.get_or_create(uri=export_path, dataset=ds)

    def export(self, export_root, metadata, data):
        export_path = os.path.join(export_root, self.gen_file_name(metadata))
        print('Export buoy #%s data to: %s' % (metadata[0], export_path))
        np.savetxt(export_path, data, header=';'.join(self.COL_NAMES), fmt='%s', delimiter=';')
        return export_path

    def get_or_create(self, metadata_uri, data_uri,
                      time_coverage_start=None,
                      time_coverage_end=None,
                      maxnum=None, minlat=-90, maxlat=90, minlon=-180, maxlon=180):
        """ Create al''l datasets from given file and add corresponding metadata

        Parameters:
        ----------
            uri_data : str
                URI to file
            uri_metadata : str
                URI to metadata file
            time_coverage_start : timezone.datetime object
                Optional start time for ingestion
            time_coverage_end : timezone.datetime object
                Optional end time for ingestion
        Returns:
        -------
            count : Number of ingested buoy datasets
        """

        source, data_center, iso = self.set_metadata()
        metadata_path = nansat_filename(metadata_uri)
        data_file = nansat_filename(data_uri)

        data = []

        # Metadata info: http://www.aoml.noaa.gov/envids/gld/general_info/dir_table.php
        # Data: http://www.aoml.noaa.gov/envids/gld/FtpMetadataInstructions.php
        metadata = self.read_metadata(metadata_path)

        # Read file with buoy data
        # Metadata info: http://www.aoml.noaa.gov/envids/gld/general_info/dir_table.php
        # Data: http://www.aoml.noaa.gov/envids/gld/general_info/krig_table.php
        # Attention! The columns in the description are not exactly correct
        cnt = 0
        with open(data_file, 'r') as data_f:
            print('Open file: %s' % data_uri)
            for line in data_f:
                line = line.strip().split()
                # If buoy id form the line is equal to buoy id from first row in metadata
                if line[0] == metadata[0][0]:
                    # Then add this line to arr
                    data.append(line)
                # Else we accumulated all information about one buoy
                # and want to process that
                else:
                    cnt += 1
                    # Extract metadata about the buoy from meta file
                    self.process_data(metadata.pop(0), data, iso, data_center, source, metadata_path)
                    data = list()

            # Add last buoy thom the input file
            cnt += 1
            self.process_data(metadata.pop(0), data, iso, data_center, source, metadata_path)

            if len(metadata) == 0:
                print('All buoys were added to the database')
            else:
                warnings.warn('Not all buoys were added to the database! Missed %s buoys' % (len(metadata)))

        return cnt
