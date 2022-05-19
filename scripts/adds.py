from odps.udf import annotate
@annotate("bigint,bigint->bigint")
class adds(object):
    def evaluate(self, arg0, arg1):
      if None in (arg0, arg1):
        return None
      return arg0 + arg1
