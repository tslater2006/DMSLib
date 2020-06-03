using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DMSLib
{
    public class EndianBinaryReader : BinaryReader
    {
        bool isLittleEndian;
        public EndianBinaryReader(Stream stream, bool littleEndian) : base(stream)
        {
            isLittleEndian = littleEndian;
        }

        public string ReadFromUnicode(int size)
        {
            /* assumption is that size will always be even... */
            if (size % 2 == 1)
            {
                throw new Exception("ReadFromUnicode must be an even number of bytes in size.");
            }

            byte[] data = ReadBytes(size);

            if (isLittleEndian)
            {
                var str = Encoding.Unicode.GetString(data);
                var nullIndex = str.IndexOf('\0');
                if (nullIndex >= 0)
                {
                    str = str.Substring(0, nullIndex);
                }
                return str;
            } else
            {
                /* need to swap the bytes... */
                for (var x = 0; x < data.Length - 1; x += 2)
                {
                    var temp = data[x];
                    data[x] = data[x + 1];
                    data[x + 1] = temp;
                }

                var str = Encoding.Unicode.GetString(data);
                var nullIndex = str.IndexOf('\0');
                if (nullIndex >= 0)
                {
                    str = str.Substring(0, nullIndex);
                }
                return str;
            }

        }

        public override short ReadInt16()
        {
            if (isLittleEndian)
            {
                return base.ReadInt16();
            }
            else
            {
                return BitConverter.ToInt16(base.ReadBytes(2).Reverse().ToArray(), 0);
            }
        }

        public override int ReadInt32()
        {
            if (isLittleEndian)
            {
                return base.ReadInt32();
            }
            else
            {
                return BitConverter.ToInt32(base.ReadBytes(4).Reverse().ToArray(), 0);
            }
        }

        public override long ReadInt64()
        {
            if (isLittleEndian)
            {
                return base.ReadInt64();
            }
            else
            {
                return BitConverter.ToInt64(base.ReadBytes(8).Reverse().ToArray(), 0);
            }
        }

        public override ushort ReadUInt16()
        {
            if (isLittleEndian)
            {
                return base.ReadUInt16();
            }
            else
            {
                return BitConverter.ToUInt16(base.ReadBytes(2).Reverse().ToArray(), 0);
            }
        }
        public override uint ReadUInt32()
        {
            if (isLittleEndian)
            {
                return base.ReadUInt32();
            }
            else
            {
                return BitConverter.ToUInt32(base.ReadBytes(4).Reverse().ToArray(), 0);
            }
        }

        public override ulong ReadUInt64()
        {
            if (isLittleEndian)
            {
                return base.ReadUInt64();
            }
            else
            {
                return BitConverter.ToUInt64(base.ReadBytes(8).Reverse().ToArray(), 0);
            }
        }
    }

}

