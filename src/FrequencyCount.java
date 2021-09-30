import java.io.Serializable;
import java.util.Date;


public class FrequencyCount implements Serializable {
	private int frequency;
	private int bucketId;
	private Date hashTagCreatedAt;
	public Date getHashTagCreatedAt() {
		return hashTagCreatedAt;
	}

	public void setHashTagCreatedAt(Date hashTagCreatedAt) {
		this.hashTagCreatedAt = hashTagCreatedAt;
	}

	public FrequencyCount()
	{
		frequency=0;
		bucketId=0;
	}
	public FrequencyCount(int frequency,int bucketId)
	{
		this.frequency=frequency;
		this.bucketId=bucketId;
	}
	public FrequencyCount(int frequency,int bucketId,Date hashTagCreatedAt)
	{
		this.frequency=frequency;
		this.bucketId=bucketId;
		this.hashTagCreatedAt=hashTagCreatedAt;
	}
	public void setFrequency(int frequency)
	{
		this.frequency=frequency;
	}
	public void setBucketId(int bucketId)
	{
		this.bucketId=bucketId;
	}
	public int getFrequency()
	{
		return this.frequency;
	}
	public int getBucketId()
	{
		return this.bucketId;
	}
}
