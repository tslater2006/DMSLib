using System.IO;

namespace DMSLib
{
    public class DMSWriter
    {
        public static void Write(string path, DMSFile file, bool saveOnlyDiffs = false)
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }

            using (StreamWriter sw = new StreamWriter(File.OpenWrite(path)))
            {
                file.WriteToStream(sw, saveOnlyDiffs);
            }
        }
    }
}