from django.core.management.base import BaseCommand

from geospaas.utils import uris_from_args
from svp_drifters.models import SVPDrifter

class Command(BaseCommand):
    args = '<metadata filename> <data filename>'
    help = 'Add drifter metadata to archive'

    def handle(self, *args, **options):
        if not len(args)>=2:
            raise IOError('Please provide two filenames')

        uris = uris_from_args(args[0], args[1])
        if len(args)==3:
            num=int(args[2])
        else:
            num=0
        count = SVPDrifter.objects.add_svp_drifters(uris[0], uris[1],
                maxnum=num)
        print 'Added %d new drifter datasets'%count

