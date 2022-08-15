FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["src/DynamicKafkaConsumer/DynamicKafkaConsumer.csproj", "DynamicKafkaConsumer/"]
RUN dotnet restore "src/DynamicKafkaConsumer/DynamicKafkaConsumer.csproj"
COPY . .
WORKDIR "/src/DynamicKafkaConsumer"
RUN dotnet build "DynamicKafkaConsumer.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "DynamicKafkaConsumer.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "DynamicKafkaConsumer.dll"]
