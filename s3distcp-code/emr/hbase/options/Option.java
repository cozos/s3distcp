package emr.hbase.options;

public abstract interface Option
{
  public abstract int matches(String[] paramArrayOfString, int paramInt);
  
  public abstract String helpLine();
  
  public abstract void require();
  
  public abstract boolean defined();
}

/* Location:
 * Qualified Name:     emr.hbase.options.Option
 * Java Class Version: 7 (51.0)
 * JD-Core Version:    0.7.1
 */