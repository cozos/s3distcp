package emr.hbase.options;

public class SimpleOption
  extends OptionBase
  implements Option
{
  public boolean value;
  
  SimpleOption(String arg, String desc)
  {
    super(arg, desc);
    value = false;
  }
  
  public int matches(String[] arguments, int matchIndex)
  {
    String argument = arguments[matchIndex];
    if (argument.equals(arg))
    {
      value = true;
      return matchIndex + 1;
    }
    return matchIndex;
  }
  
  public String helpLine()
  {
    return arg + "   -   " + desc;
  }
  
  public void require()
  {
    if (!value) {
      throw new RuntimeException("expected argument " + arg);
    }
  }
  
  public boolean defined()
  {
    return value;
  }
}

/* Location:
 * Qualified Name:     emr.hbase.options.SimpleOption
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */