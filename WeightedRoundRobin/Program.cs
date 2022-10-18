using System.Collections.Concurrent;

namespace WeightedRoundRobin;

/// <summary>
/// 权重轮询算法
/// </summary>
public static class WeightedRoundRobin
{
    private static readonly List<Server> Servers = new List<Server>()
    {
        new()
        {
            Ip = "192.168.0.100",
            Weight = 3
        },
        new()
        {
            Ip = "192.168.0.101",
            Weight = 2
        },
        new()
        {
            Ip = "192.168.0.102",
            Weight = 6
        },
        new()
        {
            Ip = "192.168.0.103",
            Weight = 4
        },
        new()
        {
            Ip = "192.168.0.104",
            Weight = 1
        },
    }.OrderBy(a => a.Weight).ToList();

    private static int _currentWeight; //当前调度的权值
    private static int _lastServer = -1; //代表上一次选择的服务器

    private static readonly int Gcd = GetGcd(Servers); //表示集合S中所有服务器权值的最大公约数
    private static readonly int MaxWeight = GetMaxWeight(Servers); // 最大权重
    private static readonly int ServerCount = Servers.Count; //服务器个数

    /// <summary>
    ///   算法流程：
    ///   假设有一组服务器 S = { S0, S1, …, Sn-1 }
    ///   变量_lastServer表示上次选择的服务器, 如上次所选为权重最大服务器, 则本次所选为权重最小服务器, 所有服务器权值的最大公约数为每次步长
    ///   权值_currentWeight初始化为0，_lastServer初始化为-1 ，当第一次的时候 权值取最大的那个服务器，
    ///   通过权重的不断递减 只要服务器权重大于等于当前权重 则服务器返回(即权重越大的服务器, 被选择的机会越多)，直到轮询结束，权值返回为0
    /// </summary>
    public static Server? GetServer()
    {
        while (true)
        {
            _lastServer = (_lastServer + 1) % ServerCount;
            if (_lastServer == 0)
            {
                _currentWeight -= Gcd;
                if (_currentWeight <= 0)
                {
                    _currentWeight = MaxWeight;
                    if (_currentWeight == 0)
                        return default;
                }
            }

            if (Servers[_lastServer].Weight >= _currentWeight)
            {
                return Servers[_lastServer];
            }
        }
    }


    /// <summary>
    /// 获取服务器所有权值的最大公约数
    /// </summary>
    private static int GetGcd(IEnumerable<Server> servers)
    {
        return servers.Select(s => s.Weight).Aggregate(GetGcd);
    }

    /// <summary>
    /// 2个数字的最大公约数
    /// </summary>
    private static int GetGcd(int a, int b)
    {
        while (true)
        {
            if (b == 0) return Math.Abs(a);
            var a1 = a;
            a = b;
            b = a1 % b;
        }
    }

    /// <summary>
    /// 获取最大的权值
    /// </summary>
    private static int GetMaxWeight(IEnumerable<Server> servers)
    {
        return servers.Max(s => s.Weight);
    }
}

/// <summary>
/// 服务器结构
/// </summary>
public class Server
{
    public string? Ip { get; set; }
    public int Weight { get; set; }
}

public class Program
{
    public static async Task Main(string[] _)
    {
        ConcurrentDictionary<string, int> dic = new();

        await Parallel.ForEachAsync(Enumerable.Range(1, 100000), new ParallelOptions { MaxDegreeOfParallelism = 1000 },
            (_, _) =>
            {
                var server = WeightedRoundRobin.GetServer();

                var key = $"服务器: {server?.Ip}, 权重: {server?.Weight}";
                Console.WriteLine(key);
                dic.AddOrUpdate(key, 0, (_, v) => v + 1);
                return ValueTask.CompletedTask;
            });

        foreach (var i1 in dic.OrderByDescending(pair => pair.Value))
            Console.WriteLine("{0}共处理请求{1}次", i1.Key, i1.Value);

        Console.ReadLine();
    }
}