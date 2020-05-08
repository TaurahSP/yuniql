namespace Yuniql.Snowflake
{
    public static class StringExtensions
    {
        public static string Quote(this string str) {
            return $"'{str}'";
        }
    }
}
