from django.core.management.base import BaseCommand

from geospaas.utils import uris_from_args
from svp_drifters.models import SVPDrifter

class Command(BaseCommand):
    args = '<metadata filename> <data filename>'
    help = 'Add drifter metadata to archive'

    def handle(self, *args, **options):
        if not len(args)==2:
            raise IOError('Please provide two filenames')

        uris = uris_from_args(*args)
        count = SVPDrifter.objects.add_svp_drifters(uris[0], uris[1])
        print 'Added %d new drifter datasets'%count

