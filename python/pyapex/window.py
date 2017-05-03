
class WindowOption(object):
    def __init__(self):
        return


class GlobalWindow(WindowOption):

    def __init__(self):
        return

class TimeWindows(WindowOption):

    durationInMillis = 0
    def __init__(self,durationInMillis ):
        if not instance(durationInMillis , int ):
            raise Exception("Incorrect data type provided")
        self.durationInMillis  = durationInMillis

    def slideBy(self, slideDurationInMillis):
        if not instance(slideDurationInMillis, int):
            raise Exception("Incorrect data type provided")
        return SlidingWindows(self.durationInMillis,slideDurationInMillis )

class SlidingWindows(TimeWindows):

    durationInMillis = 0
    slideDurationInMillis = 0
    def __init__(self, durationInMillis, slideDurationInMillis ):
        if not instance( durationInMillis, int ) or not ( slideDurationInMillis, int):
            raise Exception("Incorrect data type provided")

        self.slideDurationInMillis= slideDurationInMillis
        self.durationInMillis = durationInMillis



