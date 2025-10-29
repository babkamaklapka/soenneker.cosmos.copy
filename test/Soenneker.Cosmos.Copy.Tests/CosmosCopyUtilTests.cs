using Soenneker.Cosmos.Copy.Abstract;
using Soenneker.Tests.FixturedUnit;
using Xunit;

namespace Soenneker.Cosmos.Copy.Tests;

[Collection("Collection")]
public sealed class CosmosCopyUtilTests : FixturedUnitTest
{
    private readonly ICosmosCopyUtil _util;

    public CosmosCopyUtilTests(Fixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
        _util = Resolve<ICosmosCopyUtil>(true);
    }

    [Fact]
    public void Default()
    {

    }
}
