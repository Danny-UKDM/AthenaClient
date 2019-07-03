using System.Threading.Tasks;
using Amazon;
using Amazon.Athena;

namespace AthenaClient
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var athenaClient = new AmazonAthenaClient(new AmazonAthenaConfig { RegionEndpoint = RegionEndpoint.EUWest1 });

            var method1 = new Method1(athenaClient);
            await method1.Execute();

            var method2 = new Method2(athenaClient);
            await method2.Execute();
        }
    }
}
