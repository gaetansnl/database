using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Core.Term;
using Query;
using RocksDbSharp;
using Storage.Rocks;

namespace Test
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            var options = new DbOptions();
            options.SetCreateIfMissing();
            var storage = new RocksDbStorage(options, "gg");
            // for (int i = 0; i < 5000; i++)
            // {
            //       JsonDocument gg = JsonSerializer.Deserialize<JsonDocument>("{\"name\":\"john\",\"age\":22,\"class\":\"mca\",\"test\":{\"gg\":2.5}}");
            //       storage.Index(gg);
            // }
            // for (int i = 0; i < 10; i++)
            // {
            //     JsonDocument gg = JsonSerializer.Deserialize<JsonDocument>("{\"name\":\"john\",\"age\":23,\"class\":\"mca\",\"test\":{\"gg\":2.5}}");
            //     storage.Index(gg);
            // }
            // storage.Index(JsonSerializer.Deserialize<JsonDocument>("{\"name\":\"gaetan\",\"age\":25,\"class\":\"mca1\",\"test\":{\"gg\":2.5}}"));
            // storage.Index(JsonSerializer.Deserialize<JsonDocument>("{\"name\":\"amelia\",\"age\":24,\"class\":\"mca2\",\"test\":{\"gg\":2.5}}"));
            // storage.Index(JsonSerializer.Deserialize<JsonDocument>("{\"name\":\"test1\",\"age\":20,\"class\":\"mca3\",\"test\":{\"gg\":2.5}}"));
            // storage.Index(JsonSerializer.Deserialize<JsonDocument>("{\"name\":\"test2\",\"age\":20,\"class\":\"mca3\",\"test\":{\"gg\":2.5}}"));
            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            
            var query = new OrQuery(new List<IQuery>{new TermQuery(Term.FromLong(24)), new TermQuery(Term.FromString("mca3"))});
            var executor = query.GetExecutor(storage);
            var enumerator = executor.GetEnumerator();
            while (enumerator.MoveNext())
            {
               Console.WriteLine(enumerator.Current);
            }

            stopWatch.Stop();
            // Get the elapsed time as a TimeSpan value.
            TimeSpan ts = stopWatch.Elapsed;

            // Format and display the TimeSpan value.
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                ts.Hours, ts.Minutes, ts.Seconds,
                ts.Milliseconds / 10);
            Console.WriteLine("RunTime " + elapsedTime);
        }
    }
}