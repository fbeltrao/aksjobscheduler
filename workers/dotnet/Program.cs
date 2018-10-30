using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Threading;

namespace Worker
{
    class Program 
    {
        internal static CancellationTokenSource Shutdown = new CancellationTokenSource();
        static async Task Main (string[] args) 
        {
            try
            {
                var host = new HostBuilder()
    #if DEBUG
                    .UseEnvironment(EnvironmentName.Development)
    #endif    
                    .ConfigureAppConfiguration((hostContext, configApp) => {
                        //configApp.SetBasePath(Directory.GetCurrentDirectory ());
                        configApp.AddJsonFile("appsettings.json", optional : true);
                        configApp.AddJsonFile($"appsettings.{hostContext.HostingEnvironment.EnvironmentName}.json", optional : true);
                        configApp.AddEnvironmentVariables();
                        configApp.AddCommandLine(args);
                    })
                    .ConfigureLogging((hostContext, configLogging) => {                    
                        configLogging.AddConsole();
                        configLogging.AddDebug();
                    })
                    .ConfigureServices ((hostContext, services) => {
                        if (hostContext.HostingEnvironment.IsDevelopment()) {
                            // Development service configuration
                        } else {
                            // Non-development service configuration
                        }

                        var eventGridTopicEndpoint = hostContext.Configuration.GetValue<string>("EVENTGRIDTOPICENDPOINT");
                        var eventGridSasKey = hostContext.Configuration.GetValue<string>("EVENTGRIDSASKEY");

                        if (!string.IsNullOrEmpty(eventGridTopicEndpoint) && !string.IsNullOrEmpty(eventGridSasKey))
                        {
                            services.AddSingleton<IJobFinishedNotifier, EventGridJobFinishedNotifier>();
                        }
                        else
                        {
                            services.AddSingleton<IJobFinishedNotifier>(new NoJobFinishedNotifier());
                        }

                        services.AddHostedService<JobService>();
                    });

                await host.RunConsoleAsync(Shutdown.Token);                
            }
            catch (Exception ex)
            {                
                Console.Error.WriteLine($"Error starting job: {ex.ToString()}");
                Environment.ExitCode = 1;
            }
        }
    }
}
