package emr.hbase.options;

public class OptionWithArg
  extends OptionBase
  implements Option
{
  public String value;
  
  public OptionWithArg(String arg, String desc)
  {
    super(arg, desc);
  }
  
  public int matches(String[] arguments, int matchIndex)
  {
    String argument = arguments[matchIndex];
    if (argument.equals(arg))
    {
      if (matchIndex + 1 < arguments.length)
      {
        value = arguments[(matchIndex + 1)];
        return matchIndex + 2;
      }
      throw new RuntimeException("expected argument for " + arg + " but no argument was given");
    }
    if ((argument.length() >= arg.length() + 1) && (argument.substring(0, arg.length() + 1).equals(arg + "=")))
    {
      value = argument.substring(arg.length() + 1);
      return matchIndex + 1;
    }
    if ((argument.length() >= arg.length() + 2) && (argument.substring(0, arg.length() + 2).equals(arg + "==")))
    {
      value = argument.substring(arg.length() + 2);
      return matchIndex + 1;
    }
    return matchIndex;
  }
  
  public String helpLine()
  {
    return arg + "=VALUE   -   " + desc;
  }
  
  public boolean defined()
  {
    return value != null;
  }
  
  public String getValue(String defaultValue)
  {
    if (value != null) {
      return value;
    }
    return defaultValue;
  }
}

/* Location:
 * Qualified Name:     emr.hbase.options.OptionWithArg
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */