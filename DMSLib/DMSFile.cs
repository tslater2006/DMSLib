using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace DMSLib
{
    public class DMSFileMergeResult
    {
        public bool Success;
        public List<DMSTable> SuccessfulMerge = new List<DMSTable>();
        public List<DMSTable> NoOpMerge = new List<DMSTable>();
        public List<DMSTable> FailedMerge = new List<DMSTable>();
    }
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

        public DMSFileMergeResult[] MergeDMSFiles(DMSFile[] filesToMerge, bool dedupRows = true)
        {
            List<DMSFileMergeResult> results = new List<DMSFileMergeResult>();
            foreach (var f in filesToMerge)
            {
                results.Add(MergeDMSFile(f));
            }

            return results.ToArray();
        }

        public DMSFileMergeResult[] MergeDMSFiles(string[] filesToMerge, bool dedupRows = true)
        {
            List<DMSFileMergeResult> results = new List<DMSFileMergeResult>();
            foreach (var f in filesToMerge)
            {
                results.Add(MergeDMSFile(DMSReader.Read(f)));
            }

            return results.ToArray();
        }

        public DMSFileMergeResult MergeDMSFile(DMSFile fileToMerge, bool dedupRows = true)
        {
            DMSFileMergeResult result = new DMSFileMergeResult();
            if (this.Equals(fileToMerge))
            {
                result.Success = false;
                return result;
            }
            foreach (var newTable in fileToMerge.Tables)
            {
                Console.WriteLine("Merging table: " + newTable.Name);
                if (Tables.Select(t => t.Name).Contains(newTable.Name))
                {
                    /* make sure we can merge these rows */
                    var existingColumns = Tables.Where(t => t.Name.Equals(newTable.Name)).First().Columns.Select(c => c.Name);
                    if (newTable.Columns.Select(c=>c.Name).SequenceEqual(existingColumns)) {

                        if (dedupRows) { DedupRows(newTable); }
                        if (newTable.Rows.Count > 0)
                        {
                            Tables.Add(newTable);
                            result.SuccessfulMerge.Add(newTable);
                        } else
                        {
                            result.NoOpMerge.Add(newTable);
                        }
                    } else
                    {
                        result.FailedMerge.Add(newTable);
                    }

                } else
                {
                    /* no collision, just add it to the list */
                    if (dedupRows) { DedupRows(newTable); }
                    if (newTable.Rows.Count > 0)
                    {
                        Tables.Add(newTable);
                        result.SuccessfulMerge.Add(newTable);
                    } else
                    {
                        result.NoOpMerge.Add(newTable);
                    }

                }
            }
            result.Success = true;
            return result;
        }
        private void DedupRows(DMSTable fromTable)
        {
            List<DMSRow> rowsToRemove = new List<DMSRow>();
            bool rowRemoved = false;
            foreach (DMSRow r in fromTable.Rows)
            {
                rowRemoved = false;
                var existingTables = Tables.Where(t => t.Name == fromTable.Name).ToList();
                foreach (var eTable in existingTables)
                {
                    foreach (var eRow in eTable.Rows)
                    {
                        if (eRow.KeyHash == r.KeyHash && eRow.ValueHash == r.ValueHash)
                        {
                            rowsToRemove.Add(r);
                            rowRemoved = true;
                            break;
                        }
                    }
                    if (rowRemoved)
                    {
                        break;
                    }
                }
            }

            foreach (DMSRow r in rowsToRemove)
            {
                fromTable.Rows.Remove(r);
            }

        }

        public void WriteToStream(StreamWriter sw, bool saveOnlyDiffs = false)
        {
            /* Write out the header */
            sw.WriteLine($"SET VERSION_DAM  {Version}");
            sw.WriteLine(BlankLine);
            /* Right now we only write out LE version, though we can read BE version */
            sw.WriteLine($"SET ENDIAN LE");
            // sw.WriteLine($"SET ENDIAN {Endian}");
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

        public DDLDefaults(byte[] data, bool littleEndian)
        {
            using (MemoryStream ms = new MemoryStream(data))
            {
                using (EndianBinaryReader br = new EndianBinaryReader(ms,littleEndian))
                {
                    /* First DWORD is number of DDL Models */
                    int modelCount = br.ReadInt32();
                    Models = new DDLModel[modelCount];
                    for (var x = 0; x < modelCount; x++)
                    {
                        Models[x] = new DDLModel(br);
                    }

                    Unknown1 = br.ReadInt32();
                    var parameterCount = br.ReadInt32();
                    foreach (var model in Models)
                    {
                        for (var x = 0; x < model.Parameters.Length; x++)
                        {
                            model.Parameters[x] = new DDLParam(br);
                        }
                    }

                    var overrideCount = br.ReadInt32();
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

        public DDLModel(EndianBinaryReader br)
        {
            Unknown1 = br.ReadInt32();
            Unknown2 = br.ReadInt32();
            ParameterCount = br.ReadInt32();
            StatementType = br.ReadInt16();
            PlatformID = br.ReadInt16();

            var ddlLength = br.ReadInt32();
            ModelSQL = br.ReadFromUnicode(ddlLength);
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
    }

    public class DDLParam
    {
        public string Name;
        public short PlatformID;
        public short StatementType;
        public int Unknown1;
        public int Unknown2;
        public string Value;

        public DDLParam(EndianBinaryReader br)
        {
            Unknown1 = br.ReadInt32();
            StatementType = br.ReadInt16();
            PlatformID = br.ReadInt16();
            var nameLength = br.ReadInt32();
            Name = br.ReadFromUnicode(nameLength);
            var valueLength = br.ReadInt32();
            Value = br.ReadFromUnicode(valueLength);
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

        public TableSpaceParamOverride(EndianBinaryReader br)
        {
            SizingSet = br.ReadInt32();
            PlatformID = br.ReadInt16();
            Name = br.ReadFromUnicode(br.ReadInt32());
            Value = br.ReadFromUnicode(br.ReadInt32());
            DBName = br.ReadFromUnicode(br.ReadInt32());
            TableSpace = br.ReadFromUnicode(br.ReadInt32());
            Unknown1 = br.ReadInt32();
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
    }
}