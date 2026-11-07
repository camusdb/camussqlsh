# Development

```shell
# Build
dotnet build CamusDb.SqlSh/CamusDb.SqlSh.csproj

# Test the line editor
dotnet test Radline/RadLine.Tests/RadLine.Tests.csproj

# Run from source
dotnet run --project CamusDb.SqlSh/CamusDb.SqlSh.csproj -- northwind

# Pack into CamusDb.SqlSh/nupkg/ and install the local package
dotnet pack CamusDb.SqlSh/CamusDb.SqlSh.csproj -c Release
dotnet tool update --global CamusDB.SqlSh --add-source ./CamusDb.SqlSh/nupkg
```
