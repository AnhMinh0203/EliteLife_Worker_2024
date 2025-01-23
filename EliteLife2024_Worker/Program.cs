using EliteLife2024_Worker;

var builder = Host.CreateDefaultBuilder(args).UseWindowsService()
.ConfigureServices((hostContext, services) =>
{
    services.AddHostedService<Worker>();
    services.AddHttpClient();
});

var host = builder.Build();
host.Run();
