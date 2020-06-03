using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace DMSLib
{
    public class DMSRecordMetadata
    {
        public string AnalyticDeleteRecord;
        public int BuildSequence;

        /* 4 bytes unknown */
        public int DDLParamGroupCount;
        public int FieldCount;

        /* List of Field Info structures */
        public List<DMSRecordFieldMetadata> FieldMetadata = new List<DMSRecordFieldMetadata>();
        public int IndexCount;

        public List<DMSRecordIndexMetadata> Indexes = new List<DMSRecordIndexMetadata>();

        /* This contains the "indexes" for the record (if any), as well as tablespace info for the record */
        public byte[] LeftoverData;

        public string OptimizationTriggers;
        public string OwnerID;

        public List<DMSDDLParamGroup> ParameterGroups = new List<DMSDDLParamGroup>();
        public string ParentRecord;
        public string RecordDBName;
        public string RecordLanguage;
        public string RecordName;
        public string RelatedLanguageRecord;
        public string SystemIDField; /* 38 bytes */

        public List<DMSRecordTablespaceMetadata> Tablespaces = new List<DMSRecordTablespaceMetadata>();
        public string TimestampField; /* 38 bytes */

        /* 10 bytes unknown 00 00 01 00 00 00 00 00 00 00 */
        public byte[] Unknown2;

        /* 22 bytes unknown*/
        public byte[] Unknown4;

        public int VersionNumber;

        public int VersionNumber2;

        public DMSRecordMetadata(byte[] data, bool littleEndian)
        {
            using (MemoryStream ms = new MemoryStream(data))
            {
                using (EndianBinaryReader br = new EndianBinaryReader(ms, littleEndian))
                {
                    RecordLanguage = br.ReadFromUnicode(8);
                    OwnerID = br.ReadFromUnicode(10);
                    AnalyticDeleteRecord = br.ReadFromUnicode(32);
                    ParentRecord = br.ReadFromUnicode(32);
                    RecordName = br.ReadFromUnicode(32);
                    RelatedLanguageRecord = br.ReadFromUnicode(32);
                    RecordDBName = br.ReadFromUnicode(38);

                    TimestampField = br.ReadFromUnicode(38);
                    SystemIDField = br.ReadFromUnicode(38);

                    OptimizationTriggers = br.ReadFromUnicode(4);

                    /* Unknown 10 bytes */
                    Unknown2 = br.ReadBytes(10);
                    if (Unknown2.Sum(b => b) != 1 && Unknown2[2] != 1)
                    {
                        //Debugger.Break();
                    }

                    VersionNumber = br.ReadInt32();
                    FieldCount = br.ReadInt32();
                    BuildSequence = br.ReadInt32();
                    IndexCount = br.ReadInt32();

                    /* Unknown 4 bytes */
                    DDLParamGroupCount = br.ReadInt32();

                    VersionNumber2 = br.ReadInt32();

                    /* Unknown 22 bytes */
                    Unknown4 = br.ReadBytes(22);

                    byte[] fieldMetadata;

                    for (var x = 0; x < FieldCount; x++)
                    {
                        fieldMetadata = br.ReadBytes(106);
                        FieldMetadata.Add(new DMSRecordFieldMetadata(fieldMetadata, littleEndian));
                    }

                    /* Read in the Index headers (if any) */
                    for (var x = 0; x < IndexCount; x++)
                    {
                        var indexHeaderData = br.ReadBytes(40);
                        var index = new DMSRecordIndexMetadata(indexHeaderData, littleEndian);
                        Indexes.Add(index);
                    }

                    foreach (var index in Indexes)
                    {
                        for (var x = 0; x < index.FieldCount; x++)
                        {
                            var fieldInfo = new DMSRecordIndexField(br.ReadBytes(48), littleEndian);
                            index.Fields.Add(fieldInfo);
                        }

                        for (var x = 0; x < index.IndexParamGroupCount; x++)
                        {
                            DMSDDLParamGroup parmList = new DMSDDLParamGroup();
                            parmList.Header = new DMSDDLParamHeader(br.ReadBytes(16), littleEndian);
                            index.ParameterGroups.Add(parmList);
                        }

                        foreach (var parmGroup in index.ParameterGroups)
                        {
                            for (var y = 0; y < parmGroup.Header.Count; y++)
                            {
                                parmGroup.Parameters.Add(new DMSDDLParam(br.ReadBytes(276), littleEndian));
                            }
                        }
                    }

                    for (var x = 0; x < DDLParamGroupCount; x++)
                    {
                        DMSDDLParamGroup parmList = new DMSDDLParamGroup();
                        parmList.Header = new DMSDDLParamHeader(br.ReadBytes(16), littleEndian);
                        ParameterGroups.Add(parmList);
                    }

                    foreach (var parmGroup in ParameterGroups)
                    {
                        for (var y = 0; y < parmGroup.Header.Count; y++)
                        {
                            parmGroup.Parameters.Add(new DMSDDLParam(br.ReadBytes(276), littleEndian));
                        }
                    }

                    while (br.BaseStream.Position < br.BaseStream.Length - 1)
                    {
                        var DatabaseType = br.ReadFromUnicode(2);
                        var TableSpaceName = br.ReadFromUnicode(62);
                        var DBName = br.ReadFromUnicode(18);
                        Tablespaces.Add(new DMSRecordTablespaceMetadata()
                        { DatabaseType = DatabaseType, TablespaceName = TableSpaceName, DatabaseName = DBName });
                    }
                }
            }
        }

        internal void WriteToStream(StreamWriter sw)
        {
            var lines = DMSEncoder.EncodeDataToLines(GetBytes());
            foreach (var line in lines)
            {
                sw.WriteLine(line);
            }

            MemoryStream ms = new MemoryStream();
            foreach (DMSRecordFieldMetadata fieldmeta in FieldMetadata)
            {
                byte[] data = fieldmeta.GetBytes();
                ms.Write(data, 0, data.Length);
            }

            lines = DMSEncoder.EncodeDataToLines(ms.ToArray());
            foreach (var line in lines)
            {
                sw.WriteLine(line);
            }

            ms = new MemoryStream();
            foreach (DMSRecordIndexMetadata idx in Indexes)
            {
                byte[] headerBytes = idx.GetHeaderBytes();
                ms.Write(headerBytes, 0, headerBytes.Length);
            }

            lines = DMSEncoder.EncodeDataToLines(ms.ToArray());
            foreach (var line in lines)
            {
                sw.WriteLine(line);
            }

            foreach (DMSRecordIndexMetadata idx in Indexes)
            {
                byte[] fieldsBytes = idx.GetFieldsBytes();

                lines = DMSEncoder.EncodeDataToLines(fieldsBytes);
                foreach (var line in lines)
                {
                    sw.WriteLine(line);
                }

                if (idx.ParameterGroups.Count > 0)
                {
                    MemoryStream ms2 = new MemoryStream();
                    foreach (var group in idx.ParameterGroups)
                    {
                        byte[] headerBytes = group.GetHeaderBytes();
                        ms2.Write(headerBytes, 0, headerBytes.Length);
                    }

                    lines = DMSEncoder.EncodeDataToLines(ms2.ToArray());
                    foreach (var line in lines)
                    {
                        sw.WriteLine(line);
                    }
                }

                foreach (var group in idx.ParameterGroups)
                {
                    byte[] paramBytes = group.GetParameterBytes();
                    lines = DMSEncoder.EncodeDataToLines(paramBytes);
                    foreach (var line in lines)
                    {
                        sw.WriteLine(line);
                    }
                }
            }


            /* Write out any DDL parameter groups */
            if (ParameterGroups.Count > 0)
            {
                MemoryStream ms2 = new MemoryStream();
                foreach (var group in ParameterGroups)
                {
                    byte[] headerBytes = group.GetHeaderBytes();
                    ms2.Write(headerBytes, 0, headerBytes.Length);
                }

                lines = DMSEncoder.EncodeDataToLines(ms2.ToArray());
                foreach (var line in lines)
                {
                    sw.WriteLine(line);
                }
            }

            /* Write out each param group parameter bytes (each parameter entry in the group) */
            foreach (var group in ParameterGroups)
            {
                byte[] paramBytes = group.GetParameterBytes();
                lines = DMSEncoder.EncodeDataToLines(paramBytes);
                foreach (var line in lines)
                {
                    sw.WriteLine(line);
                }
            }

            ms = new MemoryStream();

            byte[] dbType = new byte[2];
            byte[] tableSpaceName = new byte[62];
            byte[] dbName = new byte[18];
            foreach (var tablespace in Tablespaces)
            {
                Array.Clear(dbType, 0, 2);
                Array.Clear(tableSpaceName, 0, 62);
                Array.Clear(dbName, 0, 18);
                var TableSpaceName = tablespace.TablespaceName;
                var DBName = tablespace.DatabaseName;
                var DBType = tablespace.DatabaseType;
                Encoding.Unicode.GetBytes(DBType, 0, DBType.Length, dbType, 0);
                Encoding.Unicode.GetBytes(TableSpaceName, 0, TableSpaceName.Length, tableSpaceName, 0);
                Encoding.Unicode.GetBytes(DBName, 0, DBName.Length, dbName, 0);
                ms.Write(dbType, 0, dbType.Length);
                ms.Write(tableSpaceName, 0, tableSpaceName.Length);
                ms.Write(dbName, 0, dbName.Length);
            }

            lines = DMSEncoder.EncodeDataToLines(ms.ToArray());
            foreach (var line in lines)
            {
                sw.WriteLine(line);
            }
        }

        private byte[] GetBytes()
        {
            MemoryStream ms = new MemoryStream();

            byte[] recordLang = new byte[8];
            Encoding.Unicode.GetBytes(RecordLanguage, 0, RecordLanguage.Length, recordLang, 0);

            byte[] ownerId = new byte[10];
            Encoding.Unicode.GetBytes(OwnerID, 0, OwnerID.Length, ownerId, 0);

            byte[] analyticDelete = new byte[32];
            Encoding.Unicode.GetBytes(AnalyticDeleteRecord, 0, AnalyticDeleteRecord.Length, analyticDelete, 0);

            byte[] parentRecord = new byte[32];
            Encoding.Unicode.GetBytes(ParentRecord, 0, ParentRecord.Length, parentRecord, 0);

            byte[] recName = new byte[32];
            Encoding.Unicode.GetBytes(RecordName, 0, RecordName.Length, recName, 0);

            byte[] relLang = new byte[32];
            Encoding.Unicode.GetBytes(RelatedLanguageRecord, 0, RelatedLanguageRecord.Length, relLang, 0);

            byte[] recDbName = new byte[38];
            Encoding.Unicode.GetBytes(RecordDBName, 0, RecordDBName.Length, recDbName, 0);

            byte[] optTriggers = new byte[4];
            Encoding.Unicode.GetBytes(OptimizationTriggers, 0, OptimizationTriggers.Length, optTriggers, 0);

            ms.Write(recordLang, 0, recordLang.Length);
            ms.Write(ownerId, 0, ownerId.Length);
            ms.Write(analyticDelete, 0, analyticDelete.Length);
            ms.Write(parentRecord, 0, parentRecord.Length);
            ms.Write(recName, 0, recName.Length);
            ms.Write(relLang, 0, relLang.Length);
            ms.Write(recDbName, 0, recDbName.Length);

            byte[] timestampFieldBytes = new byte[38];
            byte[] systemIDFieldBytes = new byte[38];
            Encoding.Unicode.GetBytes(TimestampField, 0, TimestampField.Length, timestampFieldBytes, 0);
            Encoding.Unicode.GetBytes(SystemIDField, 0, SystemIDField.Length, systemIDFieldBytes, 0);
            ms.Write(timestampFieldBytes, 0, timestampFieldBytes.Length);
            ms.Write(systemIDFieldBytes, 0, systemIDFieldBytes.Length);
            ms.Write(optTriggers, 0, optTriggers.Length);
            ms.Write(Unknown2, 0, Unknown2.Length);

            ms.Write(BitConverter.GetBytes(VersionNumber), 0, 4);
            ms.Write(BitConverter.GetBytes(FieldCount), 0, 4);
            ms.Write(BitConverter.GetBytes(BuildSequence), 0, 4);
            ms.Write(BitConverter.GetBytes(IndexCount), 0, 4);
            ms.Write(BitConverter.GetBytes(DDLParamGroupCount), 0, 4);
            ms.Write(BitConverter.GetBytes(VersionNumber2), 0, 4);
            ms.Write(Unknown4, 0, Unknown4.Length);


            return ms.ToArray();
        }
    }

    public class DMSRecordFieldMetadata
    {
        public int DecimalPositions;
        public GUIControls DefaultGUIControl;
        public FieldFormats FieldFormat;
        public int FieldLength;
        public string FieldName;

        public FieldTypes FieldType;
        public string RecordName;
        public int Unknown1;

        /* Something to do with Family Name and Display Name used with "Custom" format type*/
        public short Unknown2;

        public int Unknown5;
        public short Unknown6;
        public UseEditFlags UseEditMask;
        public int VersionNumber;

        public DMSRecordFieldMetadata(byte[] data, bool littleEndian)
        {
            using (MemoryStream ms = new MemoryStream(data))
            {
                using (EndianBinaryReader br = new EndianBinaryReader(ms, littleEndian))
                {
                    FieldName = br.ReadFromUnicode(38);
                    RecordName = br.ReadFromUnicode(32);
                    Unknown1 = br.ReadInt32();
                    if (Unknown1 != 0)
                    {
                        Debugger.Break();
                    }

                    VersionNumber = br.ReadInt32();
                    DecimalPositions = br.ReadInt32();

                    UseEditMask = (UseEditFlags)br.ReadInt32();

                    Unknown2 = br.ReadInt16();
                    if (Unknown2 != 0)
                    {
                        Debugger.Break();
                    }

                    FieldType = (FieldTypes)br.ReadInt16();
                    FieldFormat = (FieldFormats)br.ReadInt16();
                    FieldLength = br.ReadInt32();
                    DefaultGUIControl = (GUIControls)br.ReadInt32();

                    Unknown5 = br.ReadInt32();
                    Unknown6 = br.ReadInt16();
                    if (Unknown5 != 0 || Unknown6 != 0)
                    {
                        Debugger.Break();
                    }
                }
            }
        }

        public DMSRecordFieldMetadata(DMSNewColumn newColumn, DMSTable table)
        {
            FieldName = newColumn.FieldName;
            RecordName = table.Name;
            Unknown1 = 0;
            VersionNumber = newColumn.VersionNumber;
            DecimalPositions = newColumn.DecimalPositions;

            UseEditMask = newColumn.UseEditMask;

            Unknown2 = 0;

            FieldType = newColumn.FieldType;
            FieldFormat = newColumn.FieldFormat;
            FieldLength = newColumn.FieldLength;
            DefaultGUIControl = newColumn.DefaultGUIControl;

            Unknown5 = 0;
            Unknown6 = 0;
        }

        internal byte[] GetBytes()
        {
            MemoryStream ms = new MemoryStream();

            byte[] fieldName = new byte[38];
            byte[] recordName = new byte[32];

            Encoding.Unicode.GetBytes(FieldName, 0, FieldName.Length, fieldName, 0);
            Encoding.Unicode.GetBytes(RecordName, 0, RecordName.Length, recordName, 0);

            ms.Write(fieldName, 0, fieldName.Length);
            ms.Write(recordName, 0, recordName.Length);

            ms.Write(BitConverter.GetBytes(Unknown1), 0, 4);
            ms.Write(BitConverter.GetBytes(VersionNumber), 0, 4);
            ms.Write(BitConverter.GetBytes(DecimalPositions), 0, 4);
            ms.Write(BitConverter.GetBytes((int)UseEditMask), 0, 4);
            ms.Write(BitConverter.GetBytes(Unknown2), 0, 2);
            ms.Write(BitConverter.GetBytes((short)FieldType), 0, 2);
            ms.Write(BitConverter.GetBytes((short)FieldFormat), 0, 2);
            ms.Write(BitConverter.GetBytes(FieldLength), 0, 4);
            ms.Write(BitConverter.GetBytes((int)DefaultGUIControl), 0, 4);
            ms.Write(BitConverter.GetBytes(Unknown5), 0, 4);
            ms.Write(BitConverter.GetBytes(Unknown6), 0, 2);

            return ms.ToArray();
        }
    }

    public class DMSRecordTablespaceMetadata
    {
        public string DatabaseName;
        public string DatabaseType;
        public string TablespaceName;
    }

    public class DMSDDLParamGroup
    {
        public DMSDDLParamHeader Header;
        public List<DMSDDLParam> Parameters = new List<DMSDDLParam>();

        internal byte[] GetHeaderBytes()
        {
            MemoryStream ms = new MemoryStream();
            ms.Write(BitConverter.GetBytes(Header.DBType), 0, 4);
            ms.Write(BitConverter.GetBytes(Header.SizingSet), 0, 4);
            ms.Write(BitConverter.GetBytes(Header.Count), 0, 4);
            ms.Write(BitConverter.GetBytes(Header.Unknown1), 0, 4);

            return ms.ToArray();
        }

        internal byte[] GetParameterBytes()
        {
            MemoryStream ms = new MemoryStream();
            byte[] name = new byte[18];
            byte[] value = new byte[258];

            foreach (var param in Parameters)
            {
                Array.Clear(name, 0, 18);
                Array.Clear(value, 0, 258);
                Encoding.Unicode.GetBytes(param.Name, 0, param.Name.Length, name, 0);
                Encoding.Unicode.GetBytes(param.Value, 0, param.Value.Length, value, 0);

                ms.Write(name, 0, 18);
                ms.Write(value, 0, 258);
            }

            return ms.ToArray();
        }
    }

    public class DMSDDLParamHeader
    {
        public int Count;
        public int DBType;
        public int SizingSet;
        public int Unknown1;


        public DMSDDLParamHeader(byte[] data, bool littleEndian)
        {
            using (MemoryStream ms = new MemoryStream(data))
            {
                using (EndianBinaryReader br = new EndianBinaryReader(ms, littleEndian))
                {
                    DBType = br.ReadInt32();
                    SizingSet = br.ReadInt32();
                    Count = br.ReadInt32();
                    Unknown1 = br.ReadInt32();
                    if (Unknown1 != 0)
                    {
                        Debugger.Break();
                    }
                }
            }
        }
    }

    public class DMSDDLParam
    {
        public string Name;
        public string Value;

        public DMSDDLParam(byte[] data, bool littleEndian)
        {
            using (MemoryStream ms = new MemoryStream(data))
            {
                using (EndianBinaryReader br = new EndianBinaryReader(ms, littleEndian))
                {
                    Name = br.ReadFromUnicode(18);
                    Value = br.ReadFromUnicode(258);
                }
            }
        }
    }

    public class DMSRecordIndexMetadata
    {
        public short Active; /* 2 bytes */
        public short Cluster; /* 2 bytes */
        public short FieldCount; /* 2 bytes */

        public List<DMSRecordIndexField> Fields = new List<DMSRecordIndexField>();
        public string IndexID; /* 2 bytes */
        public int IndexParamGroupCount; /* 2 bytes */
        public RecordIndexTypes IndexType; /* 2 bytes */
        public List<DMSDDLParamGroup> ParameterGroups = new List<DMSDDLParamGroup>();
        public short PlatformALB; /* 2 bytes */
        public short PlatformDB2; /* 2 bytes */
        public short PlatformDB4; /* 2 bytes */
        public short PlatformDBX; /* 2 bytes */
        public short PlatformINF; /* 2 bytes */
        public short PlatformMSS; /* 2 bytes */
        public short PlatformORA; /* 2 bytes */
        public short PlatformSBS; /* 2 bytes */
        public short PlatformSYB; /* 2 bytes */
        public short Unique; /* 2 bytes */
        public int Unknown1; /* 2 bytes */
        public short Unknown2; /* 2 bytes */
        public int Unknown3; /* 4 bytes */

        public DMSRecordIndexMetadata(byte[] data, bool littleEndian)
        {
            using (MemoryStream ms = new MemoryStream(data))
            {
                using (EndianBinaryReader br = new EndianBinaryReader(ms, littleEndian))
                {
                    IndexID = br.ReadFromUnicode(2);
                    FieldCount = br.ReadInt16();
                    Unknown1 = br.ReadInt16();
                    if (Unknown1 != 0)
                    {
                        //Debugger.Break();
                    }

                    IndexParamGroupCount = br.ReadInt16();
                    Unknown2 = br.ReadInt16();
                    if (Unknown2 != 0)
                    {
                        //Debugger.Break();
                    }

                    IndexType = (RecordIndexTypes)br.ReadInt16();
                    Unique = br.ReadInt16();
                    Cluster = br.ReadInt16();
                    Active = br.ReadInt16();
                    PlatformSBS = br.ReadInt16();
                    PlatformDB2 = br.ReadInt16();
                    PlatformORA = br.ReadInt16();
                    PlatformINF = br.ReadInt16();
                    PlatformDBX = br.ReadInt16();
                    PlatformALB = br.ReadInt16();
                    PlatformSYB = br.ReadInt16();
                    PlatformMSS = br.ReadInt16();
                    PlatformDB4 = br.ReadInt16();
                    Unknown3 = br.ReadInt32();
                    if (Unknown3 != 0)
                    {
                        Debugger.Break();
                    }
                }
            }
        }

        internal byte[] GetHeaderBytes()
        {
            MemoryStream ms = new MemoryStream();
            ms.Write(Encoding.Unicode.GetBytes(IndexID), 0, 2);
            ms.Write(BitConverter.GetBytes(FieldCount), 0, 2);
            ms.Write(BitConverter.GetBytes(Unknown1), 0, 2);
            ms.Write(BitConverter.GetBytes(IndexParamGroupCount), 0, 2);
            ms.Write(BitConverter.GetBytes(Unknown2), 0, 2);
            ms.Write(BitConverter.GetBytes((short)IndexType), 0, 2);
            ms.Write(BitConverter.GetBytes(Unique), 0, 2);
            ms.Write(BitConverter.GetBytes(Cluster), 0, 2);
            ms.Write(BitConverter.GetBytes(Active), 0, 2);
            ms.Write(BitConverter.GetBytes(PlatformSBS), 0, 2);
            ms.Write(BitConverter.GetBytes(PlatformDB2), 0, 2);
            ms.Write(BitConverter.GetBytes(PlatformORA), 0, 2);
            ms.Write(BitConverter.GetBytes(PlatformINF), 0, 2);
            ms.Write(BitConverter.GetBytes(PlatformDBX), 0, 2);
            ms.Write(BitConverter.GetBytes(PlatformALB), 0, 2);
            ms.Write(BitConverter.GetBytes(PlatformSYB), 0, 2);
            ms.Write(BitConverter.GetBytes(PlatformMSS), 0, 2);
            ms.Write(BitConverter.GetBytes(PlatformDB4), 0, 2);
            ms.Write(BitConverter.GetBytes(Unknown3), 0, 4);

            return ms.ToArray();
        }

        internal byte[] GetFieldsBytes()
        {
            MemoryStream ms = new MemoryStream();


            foreach (DMSRecordIndexField field in Fields)
            {
                byte[] fieldBytes = field.GetBytes();
                ms.Write(fieldBytes, 0, fieldBytes.Length);
            }

            return ms.ToArray();
        }
    }

    public class DMSRecordIndexField
    {
        public int Ascending;
        public string FieldName;
        public int KeyPosition;
        public short Unknown3;

        public DMSRecordIndexField(byte[] data, bool littleEndian)
        {
            using (MemoryStream ms = new MemoryStream(data))
            {
                using (EndianBinaryReader br = new EndianBinaryReader(ms, littleEndian))
                {
                    FieldName = br.ReadFromUnicode(38);
                    KeyPosition = br.ReadInt32();
                    Ascending = br.ReadInt32();
                    Unknown3 = br.ReadInt16();
                    if (Unknown3 != 0)
                    {
                        Debugger.Break();
                    }
                }
            }
        }

        internal byte[] GetBytes()
        {
            MemoryStream ms = new MemoryStream();

            byte[] fieldName = new byte[38];
            Encoding.Unicode.GetBytes(FieldName, 0, FieldName.Length, fieldName, 0);

            ms.Write(fieldName, 0, fieldName.Length);
            ms.Write(BitConverter.GetBytes(KeyPosition), 0, 4);
            ms.Write(BitConverter.GetBytes(Ascending), 0, 4);
            ms.Write(BitConverter.GetBytes(Unknown3), 0, 2);

            return ms.ToArray();
        }
    }

    public enum FieldFormats : short
    {
        UPPER_OR_DEFAULT = 0,
        NAME = 1,
        PHONE_NORTH_AMERICA = 2,
        ZIP_NORTH_AMERICA = 3,
        SSN = 4,
        MIXEDCASE = 6,
        RAW_BINARY = 7,
        NUMBERS_ONLY = 8,
        SIN = 9,
        PHONE_INTL = 10,
        ZIP_INTL = 11,
        TIME_SECONDS = 12,
        TIME_MILLI = 13,
        CUSTOM = 14
    }

    public enum FieldTypes : short
    {
        CHAR = 0,
        LONG_CHAR = 1,
        NUMBER = 2,
        SIGNED_NUMBER = 3,
        DATE = 4,
        TIME = 5,
        DATETIME = 6,
        IMG_OR_ATTACH = 8,
        IMAGE_REF = 9,
        NOT_SET = 255
    }

    public enum GUIControls : int
    {
        EDIT_BOX = 4,
        DROPDOWN = 5,
        CHECKBOX = 7,
        RADIO_BTN = 8,
        DEFAULT = 99,
    }

    [Flags]
    public enum UseEditFlags : uint
    {
        KEY = 1,
        DUP_ORDER_KEY = 1 << 1,
        SYS_MAINT = 1 << 2,
        AUD_FLD_ADD = 1 << 3,
        ALT_SRCH_KEY = 1 << 4,
        LIST_BOX_ITEM = 1 << 5,
        DESCEND_KEY = 1 << 6,
        AUD_FLD_CHG = 1 << 7,
        REQUIRED = 1 << 8,
        XLAT_EDIT = 1 << 9,
        AUD_FLD_DEL = 1 << 10,
        SRCH_KEY = 1 << 11,
        REASONABLE_DATE_EDIT = 1 << 12,
        YES_NO_EDIT = 1 << 13,
        PROMPT_EDIT = 1 << 14,
        AUTO_UPDATE = 1 << 15,
        UNKNOWN1 = 1 << 16,
        UNKNOWN2 = 1 << 17,
        FROM_SRCH_FLD = 1 << 18,
        THRU_SRCH_FLD = 1 << 19,
        ONE_ZERO_EDIT = 1 << 20,
        DISABLE_ADV_SRCH_OPT = 1 << 21,
        UNKNOWN3 = 1 << 22,
        UNKNOWN4 = 1 << 23,
        DFLT_SRCH_FIELD = 1 << 24,
        UNKNOWN5 = 1 << 25,
        UNKNOWN6 = 1 << 26,
        SRCH_EVENT_FOR_PROMPT = 1 << 27,
        SRCH_EDIT = 1 << 29,
        ENABLE_AUTO_CMPLT_SRCH_RECORD = 1 << 30,
        PERSIST_IN_MENU = (uint)1 << 31,
    }

    public enum RecordIndexTypes : short
    {
        KEY = 1,
        ALT = 3,
        USER = 4
    }
}