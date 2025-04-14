namespace StatCodingExercise
{
    internal class Startup
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Startup...");

            ArchiveExtractor archiveExtractor = new ArchiveExtractor();
            await archiveExtractor.Extract();
        }
    }
}
