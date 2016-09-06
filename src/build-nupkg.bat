C:\Windows\Microsoft.NET\Framework\v4.0.30319\msbuild HighIronRanch.Azure.sln /t:Build /p:Configuration=Release
mkdir NuGetPackages
nuget pack .\HighIronRanch.Azure.DocumentDb\HighIronRanch.Azure.DocumentDb.csproj -Prop Configuration=Release -OutputDirectory .\NuGetPackages
nuget pack .\HighIronRanch.Azure.ServiceBus\HighIronRanch.Azure.ServiceBus.csproj -Prop Configuration=Release -OutputDirectory .\NuGetPackages
nuget pack .\HighIronRanch.Azure.ServiceBus.Contracts\HighIronRanch.Azure.ServiceBus.Contracts.csproj -Prop Configuration=Release -OutputDirectory .\NuGetPackages
nuget pack .\HighIronRanch.Azure.TableStorage\HighIronRanch.Azure.TableStorage.csproj -Prop Configuration=Release -OutputDirectory .\NuGetPackages