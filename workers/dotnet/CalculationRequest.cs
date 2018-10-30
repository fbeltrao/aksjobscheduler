using Newtonsoft.Json;


namespace Worker
{
    public class CalculationRequest 
    {
        [JsonProperty("id")]
        public string Id { get; set; }

         [JsonProperty("value1")]
        public double Value1 { get; set; }

         [JsonProperty("value2")]
        public double Value2 { get; set; }

         [JsonProperty("op")]
        public string Operation { get; set; }

        public CalculationResult Calculate()
        {
            double result = 0;
            switch (this.Operation)
            {
                case "+":
                case "add":
                    result = this.Value1 + this.Value2;
                    break;

                case "-":
                case "subtract":
                    result = this.Value1 - this.Value2;
                    break;

                case "/":
                case "divide":
                    result = this.Value1 / this.Value2;
                    break;

                case "*":
                case "times":
                    result = this.Value1 * this.Value2;
                    break;                                        
            }

            return new CalculationResult 
            {
                Id = this.Id,
                Value1 = this.Value1,
                Value2 = this.Value2,
                Operation = this.Operation,
                Result = result,
            };
        }
	
    }
}
