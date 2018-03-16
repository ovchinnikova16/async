﻿using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Patterns
{
    class Program
    {
        static void Main(string[] args)
        {
            const int BigNumber = 1000000;
            var fileName = Path.GetRandomFileName();
            using (var sw = new StreamWriter(fileName))
            {
                for (int i = 0; i < BigNumber; i++)
                    sw.WriteLine(i);
            }

            var threadIds = new int[BigNumber];
            File.ReadLines(fileName)
            .AsParallel()
            .ForAll(
                line =>
                {
                    Thread.SpinWait(10000);
                    threadIds[int.Parse(line)] = Thread.CurrentThread.ManagedThreadId;
                });
            int top, nextTop, counter = 1;
            top = threadIds[0];
            for (int i = 1; i < BigNumber; i++)
            {
                nextTop = threadIds[i];
                if (nextTop == top)
                {
                    counter++;
                }
                else
                {
                    Console.WriteLine(new string('\t', top) + counter);
                    counter = 1;
                }
                top = nextTop;
            }
            Console.WriteLine(new string('\t', top) + counter);

            File.Delete(fileName);
        }
    }
}
