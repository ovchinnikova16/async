using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    class RoundRobinClusterClient : ClusterClientBase
    {
        private readonly Random random = new Random();

        public RoundRobinClusterClient(string[] replicaAddresses)
            : base(replicaAddresses)
        {
        }

        public async override Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var taskTimeout = new TimeSpan(timeout.Ticks / ReplicaAddresses.Length);
            var randomIndex = Enumerable.Range(0, ReplicaAddresses.Length);
            //var randomIndex = Enumerable.Range(0, ReplicaAddresses.Length)
            //    .OrderBy(i => random.Next(ReplicaAddresses.Length))
            //    .ToList();


            foreach (var i in randomIndex)
            {
                var uri = ReplicaAddresses[i];
                var webRequest = CreateRequest(uri + "?query=" + query);
                Log.InfoFormat("Processing {0}", webRequest.RequestUri);
                var resultTask = ProcessRequestAsync(webRequest);
                
                await Task.WhenAny(resultTask, Task.Delay(taskTimeout));
                if (!resultTask.IsCompleted || resultTask.IsFaulted)
                    continue;
                return resultTask.Result;
            }

            return null;
        }

        protected override ILog Log
        {
            get { return LogManager.GetLogger(typeof(RandomClusterClient)); }
        }
    }
}
