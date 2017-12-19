import numpy as np
import datetime
import os
import pythesint as pti

from django.db import models
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

    def export(self, export_root, metadata, data):
        export_path = os.path.join(export_root, self.gen_file_name(metadata))
        print('Export buoy #%s data to: %s | Left %s buoys' % (metadata[0], export_path, len(metadata)))
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

        export_root = '/vagrant/shared/test_data'
        source, data_center, iso = self.set_metadata()
        metadata_file = nansat_filename(metadata_uri)
        data_file = nansat_filename(data_uri)

        convert_datetime_vctrz = np.vectorize(self.convert_datetime)
        data = []

        # Metadata info: http://www.aoml.noaa.gov/envids/gld/general_info/dir_table.php
        # Data: http://www.aoml.noaa.gov/envids/gld/FtpMetadataInstructions.php
        metadata = self.read_metadata(metadata_file)

        # Read file with buoy data
        # Metadata info: http://www.aoml.noaa.gov/envids/gld/general_info/dir_table.php
        # Data: http://www.aoml.noaa.gov/envids/gld/general_info/krig_table.php
        # Attention! The columns in the description are not exactly correct
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

                    # Extract metadata about the buoy from meta file
                    buoy = metadata.pop(0)
                    data = np.array(data)
                    # Export buoy data to csv and get path to file
                    export_path = self.export(export_root, buoy, data)
                    # Create timestamp from row data
                    timestamp = convert_datetime_vctrz(data[:, 1], data[:, 2], data[:, 3])
                    # Separate whole buoy dataset for several intervals with <chunk_duration> step
                    dt = datetime.timedelta(days=self.CHUNK_DURATION)
                    # Start and end datetime for subset
                    start = timestamp.min()
                    end = start + dt
                    while end < timestamp.max():
                        subset = data[(timestamp >= start) & (timestamp <= end)]
                        test = zip([self.shift_longitude(float(x)) for x in subset[:, 5]],
                                   [float(x) for x in subset[:, 4]])
                        geometry = LineString(test, srid=4326)
                        geoloc, geo_cr = GeographicLocation.objects.get_or_create(geometry=geometry)

                        ds, ds_cr = Dataset.objects.get_or_create(
                            entry_title='%s drifter no. %s' % (buoy[3], buoy[0]),
                            ISO_topic_category=iso,
                            data_center=data_center,
                            summary='',
                            time_coverage_start=start,
                            time_coverage_end=end,
                            source=source,
                            geographic_location=geoloc)
                        if ds_cr:
                            meta_uri, muc = DatasetURI.objects.get_or_create(uri=metadata_file, dataset=ds)
                            data_uri, duc = DatasetURI.objects.get_or_create(uri=export_path, dataset=ds)
                        start = end
                        end = end + dt
                    data = list()
        return 0
