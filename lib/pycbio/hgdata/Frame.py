# Copyright 2006-2012 Mark Diekhans
from pycbio.sys.Immutable import Immutable

def frameToPhase(frame):
    "convert a frame to a phase"
    if frame < 0:
        return -1
    elif frame == 0:
        return 0
    elif frame == 1:
        return 2
    elif frame == 2:
        return 1
    else:
        raise Exception("invalid frame: " + str(frame))

def phaseToFrame(phase):
    "convert a phase to a frame"
    if phase < 0:
        return -1
    elif phase == 0:
        return 0
    elif phase == 1:
        return 2
    elif phase == 2:
        return 1
    else:
        raise Exception("invalid phase: " + str(phase))

def frameIncr(frame, amt):
    """increment an interger frame by positive or negative amount. Frame of -1
    already returns -1."""
    if frame < 0:
        return frame  # no frame not changed
    elif amt >= 0:
        return ((frame + amt) % 3)
    else:
        amt3 = ((-amt)%3)
        return ((frame - (amt-amt3)) % 3)
        
# FIXME: not done or tested
class Frame(Immutable):
    """Immutable object the represents a frame, integer value of 0, 1, 2, or
    -1 for no frame."""
    
    __slots__ = ("val",)
    def __init__(self, val=-1):
        Immutable.__init__(self)
        assert((val >= -1) and (val <= 2))
        self.val = val
        self.mkImmutable()

    def incr(self, amt):
        """increment frame by positive or negative amount, returning a new
        Frame object.  Frame of -1 already returns -1."""
        if self.val < 0:
            return self  # no frame, not changed
        elif amt >= 0:
            return Frame((self.val + amt) % 3)
        else:
            amt3 = (-amt) % 3
            return Frame((self.val - (amt - amt3)) % 3)
        
