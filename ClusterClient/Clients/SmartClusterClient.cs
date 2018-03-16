using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ClusterClient.Clients;
using log4net;

namespace ClusterClient
{
    class SmartClusterClient : ClusterClientBase
    {
        private readonly Random random = new Random();

        public SmartClusterClient(string[] replicaAddresses)
            : base(replicaAddresses)
        {
        }

        public async override Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var taskTimeout = new TimeSpan(timeout.Ticks / ReplicaAddresses.Length);

            var index = new List<int>();
            for (int i = 0; i < ReplicaAddresses.Length; i++)
                index.Add(i);
            var randomIndex = index
                .OrderBy(i => random.Next(ReplicaAddresses.Length))
                .ToList();

            var resultTasks = new List<Task>();

            foreach (var i in randomIndex)
            {
                var uri = ReplicaAddresses[i];
                var webRequest = CreateRequest(uri + "?query=" + query);
                Log.InfoFormat("Processing {0}", webRequest.RequestUri);
                resultTasks.Add(ProcessRequestAsync(webRequest));

                var resultTask = await Task.WhenAny(resultTasks);
                await Task.WhenAll(resultTasks);
                return ((Task<string>)resultTask).Result;
            }

            throw new TimeoutException();
        }

        protected override ILog Log
        {
            get { return LogManager.GetLogger(typeof(RandomClusterClient)); }
        }
    }
}
