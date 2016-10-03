'''
Copyright 2014-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at

http://aws.amazon.com/asl/

or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
'''
import abc


class RecordProcessorBase(object):
    '''
    Base class for implementing a record processor.A RecordProcessor processes a shard in a stream.
    Its methods will be called with this pattern:

    - initialize will be called once
    - process_records will be called zero or more times
    - shutdown will be called if this MultiLangDaemon instance loses the lease to this shard
    '''
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def initialize(self, initialize_input):
        '''
        Called once by a KCLProcess before any calls to process_records

        :param amazon_kclpy.messages.InitializeInput initialize_input: Information about the
            initialization request for the record processor
        '''
        return

    @abc.abstractmethod
    def process_records(self, process_records_input):
        '''
        Called by a KCLProcess with a list of records to be processed and a checkpointer which accepts sequence numbers
        from the records to indicate where in the stream to checkpoint.

        :param amazon_kclpy.messages.ProcessRecordsInput process_records_input: the records, and metadata about the
            records.

        '''
        return

    @abc.abstractmethod
    def shutdown(self, shutdown_input):
        '''
        Called by a KCLProcess instance to indicate that this record processor should shutdown. After this is called,
        there will be no more calls to any other methods of this record processor.

        :param amazon_kclpy.messages.ShutdownInput shutdown_input: Information related to the shutdown request

        '''
        return

    version = 2


class V1toV2Processor(RecordProcessorBase):
    """
    Provides a bridge between the new v2 RecordProcessorBase, and the original RecordProcessorBase.

    This handles the conversion of the new input types to the older expected forms.  This normally shouldn't be used
    directly by record processors, since it's just a compatibility layer.

    The delegate should be a :py:class:`amazon_kclpy.kcl.RecordProcessorBase`:

    """
    def __init__(self, delegate):
        """
        Creates a new V1 to V2 record processor.

        :param amazon_kclpy.kcl.RecordProcessorBase delegate: the delegate where requests will be forwarded to
        """
        self.delegate = delegate

    def initialize(self, initialize_input):
        """
        Initializes the record processor

        :param amazon_kclpy.messages.InitializeInput initialize_input: the initialization request
        :return: None
        """
        self.delegate.initialize(initialize_input.shard_id)

    def process_records(self, process_records_input):
        """
        Expands the requests, and hands it off to the delegate for processing

        :param amazon_kclpy.messages.ProcessRecordsInput process_records_input: information about the records
            to process
        :return: None
        """
        self.delegate.process_records(process_records_input.records, process_records_input.checkpointer)

    def shutdown(self, shutdown_input):
        """
        Sends the shutdown request to the delegate

        :param amazon_kclpy.messages.ShutdownInput shutdown_input: information related to the record processor shutdown
        :return: None
        """
        self.delegate.shutdown(shutdown_input.checkpointer, shutdown_input.reason)