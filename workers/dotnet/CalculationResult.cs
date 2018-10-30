using Newtonsoft.Json;


namespace Worker
{
    public class CalculationResult 
    {
        [JsonProperty("id")]
        public string Id { get; set; }

         [JsonProperty("value1")]
        public double Value1 { get; set; }

         [JsonProperty("value2")]
        public double Value2 { get; set; }

         [JsonProperty("op")]
        public string Operation { get; set; }

         [JsonProperty("result")]
        public double Result { get; set; }
    }
}
