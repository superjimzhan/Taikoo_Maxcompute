from odps.udf import annotate
@annotate("bigint,bigint->bigint")
class udf_test(object):

    def evaluate(self, arg0, arg1):
        return arg0 + arg1
