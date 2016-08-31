import pytest

from amazon_kclpy import kcl
import io, re

# Dummy record processor
class RecordProcessor(kcl.RecordProcessorBase):
    def initialize(self, shard_id):
        pass
    def process_records(self, records, checkpointer):
        seq = records[0].get('sequenceNumber')
        try:
            checkpointer.checkpoint(seq)
        except Exception:
            #Try it one more time (this time it'll work)
            checkpointer.checkpoint(seq)

    def shutdown(self, checkpointer, reason):
       if 'TERMINATE' == reason:
           checkpointer.checkpoint()

'''
An input string which we'll feed to a file for kcl.py to read from.
'''

'''
This string is approximately what the output should look like. We remove whitespace when comparing this to what is
written to the outputfile.
'''
test_output_string = """
{"action": "status", "responseFor": "initialize"}
{"action": "checkpoint", "checkpoint": "456"}
{"action": "checkpoint", "checkpoint": "456"}
{"action": "status", "responseFor": "processRecords"}
{"action": "checkpoint", "checkpoint": null}
{"action": "status", "responseFor": "shutdown"}
"""

def _strip_all_whitespace(s):
    return re.sub('\s*', '', s)

test_input_string_for_perfect_input = """{"action":"initialize","shardId":"shardId-123"}
{"action":"processRecords","records":[{"data":"bWVvdw==","partitionKey":"cat","sequenceNumber":"456"}]}
{"action":"checkpoint","checkpoint":"456","error":"Exception"}
{"action":"checkpoint","checkpoint":"456"}
{"action":"shutdown","reason":"TERMINATE"}
{"action":"checkpoint","checkpoint":"456"}
"""
def test_kcl_py_integration_test_perfect_input():
    inputfile=io.BytesIO(test_input_string_for_perfect_input)
    outputfile=io.BytesIO()
    errorfile=io.BytesIO()
    process = kcl.KCLProcess(RecordProcessor(), inputfile=inputfile, outputfile=outputfile, errorfile=errorfile)
    process.run()
    '''
    The strings are approximately the same, modulo whitespace.
    '''
    assert _strip_all_whitespace(outputfile.getvalue()) == _strip_all_whitespace(test_output_string)
    '''
    There should be some error output but it seems like overly specific to make sure that a particular message is printed.
    '''
    assert str(errorfile.getvalue()) == ""
