using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DMSLib
{
    public class DMSRow
    {
        public DMSCompareResult CompareResult = DMSCompareResult.NONE;
        public byte[] Values;
        public int[] Indexes;
        public FieldTypes[] Types;
        public void InsertValueString(int index, string val)
        {
            var currentValues = new List<string>(GetValuesAsString());
            currentValues.Insert(index, val);
            CollapseValues(currentValues);
        }

        public void DeleteValue(int index)
        {
            var currentValues = new List<string>(GetValuesAsString());
            currentValues.RemoveAt(index);
            CollapseValues(currentValues);
        }
        public void ChangeValue(int index, string val)
        {
            var currentValues = new List<string>(GetValuesAsString());
            currentValues[index] = val;
            CollapseValues(currentValues);
        }
        private void CollapseValues(List<string> vals)
        {
            var newIndexes = new List<int>();
            MemoryStream ms = new MemoryStream();

            foreach(string val in vals)
            {
                byte[] valBytes;
                /* Determine if a binary value by looking for B{...} */
                if (val[0] == 'B' && val[1] == '{' && val[val.Length-1] == '}')
                {
                    valBytes = HexStringToBytes(val.Substring(2,val.Length-3));
                }else
                {
                    valBytes = Encoding.UTF8.GetBytes(val);
                }
                newIndexes.Add((int)ms.Length);
                ms.Write(valBytes, 0, valBytes.Length);
            }
            Values = ms.ToArray();
            ms.Dispose();
            Indexes = newIndexes.ToArray();
            newIndexes.Clear();
        }

        private static byte[] HexStringToBytes(string s)
        {
            const string HEX_CHARS = "0123456789ABCDEF";

            if (s.Length == 0)
                return new byte[0];

            if (s.Length % 2 != 0)
                throw new FormatException();

            byte[] bytes = new byte[s.Length / 2];

            int state = 0; // 0 = expect first digit, 1 = expect second digit, 2 = expect hyphen
            int currentByte = 0;
            int x;
            int value = 0;

            foreach (char c in s)
            {
                switch (state)
                {
                    case 0:
                        x = HEX_CHARS.IndexOf(Char.ToUpperInvariant(c));
                        if (x == -1)
                            throw new FormatException();
                        value = x << 4;
                        state = 1;
                        break;
                    case 1:
                        x = HEX_CHARS.IndexOf(Char.ToUpperInvariant(c));
                        if (x == -1)
                            throw new FormatException();
                        bytes[currentByte++] = (byte)(value + x);
                        state = 2;
                        break;
                }
            }

            return bytes;
        }

        public string GetStringValue(int index)
        {
            var start = Indexes[index];
            var end = Indexes[index + 1];
            var data = new byte[end - start];
            Array.Copy(Values, start, data, 0, end - start);
            var utf8Enc = Encoding.UTF8.GetString(data);
            if (Encoding.UTF8.GetBytes(utf8Enc).Length == data.Length)
            {
                return utf8Enc;
            }
            else
            {
                return "B{" + BitConverter.ToString(data).Replace("-", "") + "}";
            }
        }
        public dynamic GetValue(int index)
        {
            switch (Types[index])
            {
                case FieldTypes.CHAR:
                    return GetStringValue(index);
                case FieldTypes.LONG_CHAR:
                    return GetStringValue(index);
                case FieldTypes.NUMBER:
                    return Int64.Parse(GetStringValue(index));
                case FieldTypes.SIGNED_NUMBER:
                    return Int64.Parse(GetStringValue(index));
                case FieldTypes.DATE:
                    if (GetStringValue(index) == "\0")
                    {
                        return null;
                    }
                    return DateTime.ParseExact(GetStringValue(index), "yyyy-MM-dd", CultureInfo.InvariantCulture);
                case FieldTypes.DATETIME:
                    if (GetStringValue(index) == "\0")
                    {
                        return null;
                    }
                    return DateTime.ParseExact(GetStringValue(index), "yyyy-MM-dd-HH.mm.ss.000000", CultureInfo.InvariantCulture);
                case FieldTypes.TIME:
                    if (GetStringValue(index) == "\0")
                    {
                        return null;
                    }
                    return DateTime.ParseExact(GetStringValue(index), "HH.mm.ss.000000", CultureInfo.InvariantCulture);
                case FieldTypes.IMG_OR_ATTACH:
                    var start = Indexes[index];
                    var end = Indexes[index + 1];
                    var data = new byte[end - start];
                    Array.Copy(Values, start, data, 0, end - start);
                    return data;
                default:
                    Debugger.Break();
                    return null;
            }
        }
        public string[] GetValuesAsString()
        {
            string[] values = new string[Indexes.Length - 1];
            for(var x = 0; x < Indexes.Length - 1; x++)
            {
                values[x] = GetStringValue(x);
            }

            return values;
        }

        internal void WriteToStream(StreamWriter sw)
        {
            StringBuilder sb = new StringBuilder();

            for(var x = 0; x < Indexes.Length-1;x++)
            {
                var start = Indexes[x];
                var end = Indexes[x + 1];
                var data = new byte[end - start];
                Array.Copy(Values, start, data, 0, end - start);
                var strEnc = DMSEncoder.EncodeData(data);
                sb.Append(strEnc).Append(",");
            }
            
            var encodedLines = DMSEncoder.FormatEncodedData(sb.ToString());

            foreach(var line in encodedLines)
            {
                sw.WriteLine(line);
            }

            sw.WriteLine("//");
        }

        internal void SetFieldTypes(DMSTable table)
        {
            Types = new FieldTypes[table.Metadata.FieldMetadata.Count];
            for(var x = 0; x < table.Metadata.FieldMetadata.Count; x++)
            {
                Types[x] = table.Metadata.FieldMetadata[x].FieldType;
            }
        }
    }
}
