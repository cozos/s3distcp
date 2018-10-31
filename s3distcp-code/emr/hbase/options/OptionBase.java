package emr.hbase.options;

public abstract class OptionBase
  implements Option
{
  protected String arg;
  protected String desc;
  
  public OptionBase(String arg, String desc)
  {
    this.arg = arg;
    this.desc = desc;
  }
  
  public void require()
  {
    if (!defined()) {
      throw new RuntimeException("expected argument " + arg);
    }
  }
}

/* Location:
 * Qualified Name:     emr.hbase.options.OptionBase
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */