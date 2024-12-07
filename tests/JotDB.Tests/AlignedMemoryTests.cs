using JotDB.Storage;

namespace JotDB.Tests;

public class AlignedMemoryTests
{
    [Fact]
    public void Allocation_Should_Return_Requested_Size()
    {
        var memory = AlignedMemory.Allocate(
            size: 4096,
            alignment: 4096);

        memory.Span.Length.Should().Be(4096);
    }

    [Fact]
    public void Can_Free_Memory()
    {
        var memory = AlignedMemory.Allocate(
            size: 4096,
            alignment: 4096);
       
        memory.Span.Fill(255);

        memory.Dispose();
        
        1.Should().Be(1);
    }
}