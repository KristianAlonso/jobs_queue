using JobsQueue;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();

Microsoft.Extensions.Hosting.WindowsServiceLifetimeHostBuilderExtensions.AddWindowsService(builder.Services);
