import pytest, json, io
from mock import Mock, patch
from amazon_kclpy import kcl

def build_basic_io_handler_mock(read_line_side_effects):
    io_handler = Mock()
    io_handler.read_line.side_effect = read_line_side_effects
    io_handler.load_action.side_effect = lambda x : json.loads(x)
    return io_handler

def test_checkpointer_exception():
    exception_name = 'ThisIsATestException'
    checkpointer = kcl.Checkpointer(
        build_basic_io_handler_mock(['{"action":"checkpoint","checkpoint":"456", "error" : "'+exception_name+'"}']))
    try:
        checkpointer.checkpoint()
        assert 0, "Checkpointing should have raised an exception"
    except kcl.CheckpointError as e:
        assert e.value == exception_name

def test_checkpointer_unexpected_message_after_checkpointing():
    io_handler = Mock()
    io_handler.read_line.side_effect = [
        '{"action":"initialize", "shardId" : "shardid-123"}',]
    io_handler.load_action.side_effect = lambda x : json.loads(x)
    checkpointer = kcl.Checkpointer(
        build_basic_io_handler_mock(
            ['{"action":"initialize", "shardId" : "shardid-123"}']))

    try:
        checkpointer.checkpoint()
        assert 0, "Checkpointing should have raised an exception"
    except kcl.CheckpointError as e:
        assert e.value == 'InvalidStateException'

def test_kcl_process_exits_on_record_processor_exception():
    unique_string = "Super uniqe statement we can look for"
    errorFile = io.BytesIO()
    class ClientException(Exception):
        pass
    mock_rp = Mock()
    # Our record processor will just fail during initialization
    mock_rp.initialize.side_effect = [ClientException(unique_string)]
    kcl_process = kcl.KCLProcess(mock_rp,
                             inputfile=io.BytesIO('{"action":"initialize", "shardId" : "shardid-123"}'),
                             outputfile=io.BytesIO(),
                             errorfile=errorFile)
    try:
        kcl_process.run()
    except ClientException:
        assert 0, "Should not have seen the ClientException propagate up the call stack."
    assert errorFile.getvalue().count(unique_string) > 0, 'We should see our error message printed to the error file'

def test_kcl_process_exits_on_action_message_exception():
    mock_rp = Mock()
    # Our record processor will just fail during initialization
    kcl_process = kcl.KCLProcess(mock_rp,
                                 # This will suffice because a checkpoint message won't be understood by
                                 # the KCLProcessor (only the Checkpointer understands them)
                             inputfile=io.BytesIO('{"action":"checkpoint", "error" : "badstuff"}'),
                             outputfile=io.BytesIO(),
                             errorfile=io.BytesIO())
    try:
        kcl_process.run()
        assert 0, 'Should have received an exception here'
    except kcl.MalformedAction:
        pass

def test_record_processor_base():
    class NoInit(kcl.RecordProcessorBase):
        def process_records(records, checkpointer):
            pass
        def shutdown(checkpointer, reason):
            pass
    try:
        no_init = NoInit()
        assert 0, 'Should require an initialize method'
    except TypeError as e:
        pass
    class NoProc(kcl.RecordProcessorBase):
        def initialize(shard_id):
            pass
        def shutdown(checkpointer, reason):
            pass
    try:
        no_proc = NoProc()
        assert 0, 'Should require a process_records method'
    except TypeError as e:
        pass
    class NoShut(kcl.RecordProcessorBase):
        def initialize(shard_id):
            pass
        def process_records(records, checkpointer):
            pass
    try:
        no_shut = NoShut()
        assert 0, 'Should require a shutdown method'
    except TypeError as e:
        pass
