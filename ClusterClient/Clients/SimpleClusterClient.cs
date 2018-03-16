using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    class SimpleClusterClient : ClusterClientBase
    {
        public SimpleClusterClient(string[] replicaAddresses)
            : base(replicaAddresses)
        {
        }

        public async override Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var tasks = new List<Task>();
            for (int i = 0; i < ReplicaAddresses.Length; i++)
            {
                var uri = ReplicaAddresses[i];

                var webRequest = CreateRequest(uri + "?query=" + query);
                Log.InfoFormat("Processing {0}", webRequest.RequestUri);
                tasks.Add(ProcessRequestAsync(webRequest));
            }

            //foreach (var task in tasks)
            //{
            //    await Task.WhenAny(task, Task.Delay(timeout));
            //    if (!task.IsCompleted || task.IsFaulted)
            //        continue;
            //    return task.Result;
            //}
            //return null;

            var delay = Task.Delay(timeout);
            tasks.Add(delay);

            while (true)
            {
                var resultTask = await Task.WhenAny(tasks);
                if (resultTask == delay)
                    throw new TimeoutException();

                if (resultTask.IsFaulted)
                {
                    tasks.Remove(resultTask);
                    continue;
                }

                return ((Task<string>) resultTask).Result;
            }
        }

        protected override ILog Log
        {
            get { return LogManager.GetLogger(typeof(RandomClusterClient)); }
        }

    }
}
