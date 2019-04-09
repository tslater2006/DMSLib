using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace DMSLib
{
    public class DMSFile
    {
        public string BaseLanguage;
        public string BlankLine;
        public string Database;

        public DDLDefaults DDLs;
        public string Ended;
        public string Endian;
        public string FileName;
        public List<string> Namespaces = new List<string>();
        public string Started;

        public List<DMSTable> Tables = new List<DMSTable>();
        public string Version;

        public void WriteToStream(StreamWriter sw, bool saveOnlyDiffs = false)
        {
            /* Write out the header */
            sw.WriteLine($"SET VERSION_DAM  {Version}");
            sw.WriteLine(BlankLine);
            sw.WriteLine($"SET ENDIAN {Endian}");
            sw.WriteLine($"SET BASE_LANGUAGE {BaseLanguage}");
            sw.WriteLine($"REM Database: {Database}");
            sw.WriteLine($"REM Started: {Started}");

            /* Write out the namespaces */
            sw.WriteLine("EXPORT  RECORD/SPACE.x");
            foreach (var space in Namespaces)
            {
                sw.WriteLine(space);
            }

            sw.WriteLine("/");

            var metadataLines = DMSEncoder.EncodeDataToLines(DDLs.GetBytes());
            foreach (var line in metadataLines)
            {
                sw.WriteLine(line);
            }

            sw.WriteLine("/");
            foreach (var table in Tables)
            {
                if (saveOnlyDiffs)
                {
                    if (table.CompareResult == DMSCompareResult.NONE || table.CompareResult == DMSCompareResult.SAME)
                    {
                        /* skip */
                        continue;
                    }
                }

                table.WriteToStream(sw, saveOnlyDiffs);
            }

            sw.WriteLine($"REM Ended: {Ended}");
        }
    }

    public class DDLDefaults
    {
        public DDLModel[] Models;
        public TableSpaceParamOverride[] Overrides;
        public int Unknown1;

        public DDLDefaults(byte[] data)
        {
            using (MemoryStream ms = new MemoryStream(data))
            {
                using (BinaryReader br = new BinaryReader(ms))
                {
                    /* First DWORD is number of DDL Models */
                    int modelCount = BitConverter.ToInt32(br.ReadBytes(4), 0);
                    Models = new DDLModel[modelCount];
                    for (var x = 0; x < modelCount; x++)
                    {
                        Models[x] = new DDLModel(br);
                    }

                    Unknown1 = BitConverter.ToInt32(br.ReadBytes(4), 0);
                    var parameterCount = BitConverter.ToInt32(br.ReadBytes(4), 0);
                    foreach (var model in Models)
                    {
                        for (var x = 0; x < model.Parameters.Length; x++)
                        {
                            model.Parameters[x] = new DDLParam(br);
                        }
                    }

                    var overrideCount = BitConverter.ToInt32(br.ReadBytes(4), 0);
                    Overrides = new TableSpaceParamOverride[overrideCount];
                    for (var x = 0; x < overrideCount; x++)
                    {
                        Overrides[x] = new TableSpaceParamOverride(br);
                    }
                }
            }
        }

        public byte[] GetBytes()
        {
            byte[] bytes = new byte[0];
            using (MemoryStream ms = new MemoryStream())
            {
                using (BinaryWriter bw = new BinaryWriter(ms))
                {
                    bw.Write(Models.Length);
                    foreach (var model in Models)
                    {
                        model.WriteBytes(bw);
                    }

                    bw.Write(Unknown1);
                    bw.Write(Models.Sum(m => m.Parameters.Count()));
                    foreach (var model in Models)
                    {
                        foreach (var param in model.Parameters)
                        {
                            param.WriteBytes(bw);
                        }
                    }

                    bw.Write(Overrides.Length);
                    foreach (var over in Overrides)
                    {
                        over.WriteBytes(bw);
                    }
                }

                bytes = ms.ToArray();
            }

            return bytes;
        }
    }

    public class DDLModel
    {
        public string ModelSQL;
        public int ParameterCount;

        public DDLParam[] Parameters;
        public short PlatformID;
        public short StatementType;
        public int Unknown1;
        public int Unknown2;

        public DDLModel(BinaryReader br)
        {
            Unknown1 = br.ReadInt32();
            Unknown2 = br.ReadInt32();
            ParameterCount = br.ReadInt32();
            StatementType = br.ReadInt16();
            PlatformID = br.ReadInt16();

            var ddlLength = br.ReadInt32();
            ModelSQL = FromUnicodeBytes(br.ReadBytes(ddlLength));
            Parameters = new DDLParam[ParameterCount];
        }

        internal void WriteBytes(BinaryWriter bw)
        {
            bw.Write(Unknown1);
            bw.Write(Unknown2);
            bw.Write(ParameterCount);
            bw.Write(StatementType);
            bw.Write(PlatformID);
            bw.Write(ModelSQL.Length * 2 + 2);
            bw.Write(Encoding.Unicode.GetBytes(ModelSQL));
            bw.Write((short) 0);
        }

        private string FromUnicodeBytes(byte[] data)
        {
            var str = Encoding.Unicode.GetString(data);
            var nullIndex = str.IndexOf('\0');
            if (nullIndex >= 0)
            {
                str = str.Substring(0, nullIndex);
            }

            return str;
        }
    }

    public class DDLParam
    {
        public string Name;
        public short PlatformID;
        public short StatementType;
        public int Unknown1;
        public int Unknown2;
        public string Value;

        public DDLParam(BinaryReader br)
        {
            Unknown1 = br.ReadInt32();
            StatementType = br.ReadInt16();
            PlatformID = br.ReadInt16();
            var nameLength = br.ReadInt32();
            Name = FromUnicodeBytes(br.ReadBytes(nameLength));
            var valueLength = br.ReadInt32();
            Value = FromUnicodeBytes(br.ReadBytes(valueLength));
            Unknown2 = br.ReadInt32();
            Console.WriteLine($"Found Parameter {Name} with value {Value}");
        }

        public void WriteBytes(BinaryWriter bw)
        {
            bw.Write(Unknown1);
            bw.Write(StatementType);
            bw.Write(PlatformID);
            bw.Write(Name.Length * 2 + 2);
            bw.Write(Encoding.Unicode.GetBytes(Name));
            bw.Write((short) 0);
            bw.Write(Value.Length * 2 + 2);
            bw.Write(Encoding.Unicode.GetBytes(Value));
            bw.Write((short) 0);
            bw.Write(Unknown2);
        }

        private string FromUnicodeBytes(byte[] data)
        {
            var str = Encoding.Unicode.GetString(data);
            var nullIndex = str.IndexOf('\0');
            if (nullIndex >= 0)
            {
                str = str.Substring(0, nullIndex);
            }

            return str;
        }
    }

    public class TableSpaceParamOverride
    {
        public string DBName;
        public string Name;
        public short PlatformID;
        public int SizingSet;
        public string TableSpace;
        public int Unknown1;
        public string Value;

        public TableSpaceParamOverride(BinaryReader br)
        {
            SizingSet = BitConverter.ToInt32(br.ReadBytes(4), 0);
            PlatformID = BitConverter.ToInt16(br.ReadBytes(2), 0);
            Name = FromUnicodeBytes(br.ReadBytes(BitConverter.ToInt32(br.ReadBytes(4), 0)));
            Value = FromUnicodeBytes(br.ReadBytes(BitConverter.ToInt32(br.ReadBytes(4), 0)));
            DBName = FromUnicodeBytes(br.ReadBytes(BitConverter.ToInt32(br.ReadBytes(4), 0)));
            TableSpace = FromUnicodeBytes(br.ReadBytes(BitConverter.ToInt32(br.ReadBytes(4), 0)));
            Unknown1 = BitConverter.ToInt32(br.ReadBytes(4), 0);
        }

        internal void WriteBytes(BinaryWriter bw)
        {
            bw.Write(SizingSet);
            bw.Write(PlatformID);

            bw.Write(Name.Length * 2 + 2);
            bw.Write(Encoding.Unicode.GetBytes(Name));
            bw.Write((short) 0);

            bw.Write(Value.Length * 2 + 2);
            bw.Write(Encoding.Unicode.GetBytes(Value));
            bw.Write((short) 0);

            bw.Write(DBName.Length * 2 + 2);
            bw.Write(Encoding.Unicode.GetBytes(DBName));
            bw.Write((short) 0);

            bw.Write(TableSpace.Length * 2 + 2);
            bw.Write(Encoding.Unicode.GetBytes(TableSpace));
            bw.Write((short) 0);

            bw.Write(Unknown1);
        }

        private string FromUnicodeBytes(byte[] data)
        {
            var str = Encoding.Unicode.GetString(data);
            var nullIndex = str.IndexOf('\0');
            if (nullIndex >= 0)
            {
                str = str.Substring(0, nullIndex);
            }

            return str;
        }
    }
}