#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

from .TProtocol import TType, TProtocolBase, TProtocolException, TProtocolFactory
from struct import pack, unpack


class TBinaryProtocol(TProtocolBase):
    """Binary implementation of the Thrift protocol driver."""

    # NastyHaxx. Python 2.4+ on 32-bit machines forces hex constants to be
    # positive, converting this into a long. If we hardcode the int value
    # instead it'll stay in 32 bit-land.

    # VERSION_MASK = 0xffff0000
    VERSION_MASK = -65536

    # VERSION_1 = 0x80010000
    VERSION_1 = -2147418112

    TYPE_MASK = 0x000000ff

    def __init__(self, trans, strictRead=False, strictWrite=True, **kwargs):
        TProtocolBase.__init__(self, trans)
        print("TBinaryProtocol-init")
        self.strictRead = strictRead
        self.strictWrite = strictWrite
        self.string_length_limit = kwargs.get('string_length_limit', None)
        self.container_length_limit = kwargs.get('container_length_limit', None)

    def _check_string_length(self, length):
        self._check_length(self.string_length_limit, length)

    def _check_container_length(self, length):
        self._check_length(self.container_length_limit, length)

    def writeMessageBegin(self, name, type, seqid):
        if self.strictWrite:
            self.writeI32(TBinaryProtocol.VERSION_1 | type)
            self.writeString(name)
            self.writeI32(seqid)
        else:
            self.writeString(name)
            self.writeByte(type)
            self.writeI32(seqid)

    def writeMessageEnd(self):
        pass

    def writeStructBegin(self, name):
        pass

    def writeStructEnd(self):
        pass

    def writeFieldBegin(self, name, type, id):
        self.writeByte(type)
        self.writeI16(id)

    def writeFieldEnd(self):
        pass

    def writeFieldStop(self):
        self.writeByte(TType.STOP)

    def writeMapBegin(self, ktype, vtype, size):
        self.writeByte(ktype)
        self.writeByte(vtype)
        self.writeI32(size)

    def writeMapEnd(self):
        pass

    def writeListBegin(self, etype, size):
        self.writeByte(etype)
        self.writeI32(size)

    def writeListEnd(self):
        pass

    def writeSetBegin(self, etype, size):
        self.writeByte(etype)
        self.writeI32(size)

    def writeSetEnd(self):
        pass

    def writeBool(self, bool):
        if bool:
            self.writeByte(1)
        else:
            self.writeByte(0)

    def writeByte(self, byte):
        buff = pack("!b", byte)
        self.trans.write(buff)

    def writeI16(self, i16):
        buff = pack("!h", i16)
        self.trans.write(buff)

    def writeI32(self, i32):
        # print("TBinaryProtocol-writeI32 : " + str(i32))
        # print(i32)
        # print(type(i32))
        if type(i32) != int:
            i32 = 0
        buff = pack("!i", i32)
        # print("TBinaryProtocol-writeI32" + str(buff))
        self.trans.write(buff)

    def writeI64(self, i64):
        buff = pack("!q", i64)
        self.trans.write(buff)

    def writeDouble(self, dub):
        buff = pack("!d", dub)
        self.trans.write(buff)

    def writeBinary(self, str):
        self.writeI32(len(str))
        self.trans.write(str)

    def readMessageBegin(self):
        sz0 = self.readI32()
        # print("TBinaryProtocol-readMessageBegin-0sz:" + str(sz0))
        if sz0 == 0:
            return ('',0,0)
        elif sz0 == 16777216:
            sz = self.readByte()
            if sz == 0:
                return ('',0,0)
        elif sz0 != 16:
            sz = self.readI32()
            if sz == 0:
                return ('',0,0)
        else:
            sz = sz0
        
        # sz = self.readI32()
        # if sz == 0:
        #     return ('',0,0)
    
        # print(self.trans) <thrift.transport.TTransport.TBufferedTransport
        # print("TBinaryProtocol-readMessageBegin-sz:" + str(sz))
        if sz < 0:
            version = sz & TBinaryProtocol.VERSION_MASK
            print(version)
            # if version != TBinaryProtocol.VERSION_1:
            #     raise TProtocolException(
            #         type=TProtocolException.BAD_VERSION,
            #         message='Bad version in readMessageBegin: %d' % (sz))
            type = sz & TBinaryProtocol.TYPE_MASK
            print("TBinaryProtocol-readMessageBegin-type:" + str(type))
            # name = self.readString()
            name = self.trans.readAll(sz)
            print("TBinaryProtocol-readMessageBegin-name" + name.decode('utf-8'))
            seqid = self.readI32()
            print("TBinaryProtocol-readMessageBegin-seqid" + str(seqid))
        else:
            print(self.strictRead)
            if self.strictRead:
                raise TProtocolException(type=TProtocolException.BAD_VERSION,
                                         message='No protocol version header')
            name = self.trans.readAll(sz)
            # name = name.decode('utf-8')
            name = name.decode('ISO-8859-1')
            print("TBinaryProtocol-readMessageBegin-name:" + name)
            # type = self.readByte()
            type = self.readI16()
            print("TBinaryProtocol-readMessageBegin-type:" + str(type))
            # seqid = self.readI32()
            seqid = self.readI16()
            print("TBinaryProtocol-readMessageBegin-seqid:" + str(seqid))
        return (name, type, seqid)

    def readMessageEnd(self):
        pass

    def readStructBegin(self):
        pass

    def readStructEnd(self):
        pass

    def readFieldBegin(self):
        type = self.readByte()
        # type = self.readI16()
        # print("TBinaryProtocol-readFieldBegin:" + str(type))
        if type == TType.STOP:
            return (None, type, 0)
        id = self.readI16()
        return (None, type, id)

    def readFieldEnd(self):
        pass

    def readMapBegin(self):
        ktype = self.readByte()
        vtype = self.readByte()
        size = self.readI32()
        self._check_container_length(size)
        return (ktype, vtype, size)

    def readMapEnd(self):
        pass

    def readListBegin(self):
        etype = self.readByte()
        size = self.readI32()
        self._check_container_length(size)
        return (etype, size)

    def readListEnd(self):
        pass

    def readSetBegin(self):
        etype = self.readByte()
        size = self.readI32()
        self._check_container_length(size)
        return (etype, size)

    def readSetEnd(self):
        pass

    def readBool(self):
        byte = self.readByte()
        if byte == 0:
            return False
        return True

    def readByte(self):
        # print("TBinaryProtocol-readByte")
        # print(self.trans) <thrift.transport.TTransport.TSaslClientTransport
        buff = self.trans.readAll(1)
        # print("TBinaryProtocol-readByteBuff")
        val, = unpack('!b', buff)
        # print("TBinaryProtocol-readByteEnd:" + str(val))
        return val

    def readI16(self):
        buff = self.trans.readAll(2)
        if len(buff) == 0:
            return 0
        val, = unpack('!h', buff)
        return val

    def readI32(self):
        # print("TBinaryProtocol-readI32 start ")
        buff = self.trans.readAll(4)
        # print("TBinaryProtocol-readI32: " + str(buff))
        if len(buff) == 0:
            return 0
        val, = unpack('!i', buff)
        return val

    def readI64(self):
        buff = self.trans.readAll(8)
        val, = unpack('!q', buff)
        return val

    def readDouble(self):
        buff = self.trans.readAll(8)
        val, = unpack('!d', buff)
        return val

    def readBinary(self):
        size = self.readI32()
        self._check_string_length(size)
        s = self.trans.readAll(size)
        return s


class TBinaryProtocolFactory(TProtocolFactory):
    def __init__(self, strictRead=False, strictWrite=True, **kwargs):
        self.strictRead = strictRead
        self.strictWrite = strictWrite
        self.string_length_limit = kwargs.get('string_length_limit', None)
        self.container_length_limit = kwargs.get('container_length_limit', None)

    def getProtocol(self, trans):
        prot = TBinaryProtocol(trans, self.strictRead, self.strictWrite,
                               string_length_limit=self.string_length_limit,
                               container_length_limit=self.container_length_limit)
        return prot


class TBinaryProtocolAccelerated(TBinaryProtocol):
    """C-Accelerated version of TBinaryProtocol.

    This class does not override any of TBinaryProtocol's methods,
    but the generated code recognizes it directly and will call into
    our C module to do the encoding, bypassing this object entirely.
    We inherit from TBinaryProtocol so that the normal TBinaryProtocol
    encoding can happen if the fastbinary module doesn't work for some
    reason.  (TODO(dreiss): Make this happen sanely in more cases.)
    To disable this behavior, pass fallback=False constructor argument.

    In order to take advantage of the C module, just use
    TBinaryProtocolAccelerated instead of TBinaryProtocol.

    NOTE:  This code was contributed by an external developer.
           The internal Thrift team has reviewed and tested it,
           but we cannot guarantee that it is production-ready.
           Please feel free to report bugs and/or success stories
           to the public mailing list.
    """
    pass

    def __init__(self, *args, **kwargs):
        fallback = kwargs.pop('fallback', True)
        super(TBinaryProtocolAccelerated, self).__init__(*args, **kwargs)
        try:
            from thrift.protocol import fastbinary
        except ImportError:
            if not fallback:
                raise
        else:
            self._fast_decode = fastbinary.decode_binary
            self._fast_encode = fastbinary.encode_binary


class TBinaryProtocolAcceleratedFactory(TProtocolFactory):
    def __init__(self,
                 string_length_limit=None,
                 container_length_limit=None,
                 fallback=True):
        self.string_length_limit = string_length_limit
        self.container_length_limit = container_length_limit
        self._fallback = fallback

    def getProtocol(self, trans):
        return TBinaryProtocolAccelerated(
            trans,
            string_length_limit=self.string_length_limit,
            container_length_limit=self.container_length_limit,
            fallback=self._fallback)
