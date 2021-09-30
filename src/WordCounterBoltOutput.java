import java.io.Serializable;
import java.util.Date;


public class WordCounterBoltOutput implements Serializable {
	private Date hashTagCreatedAt;
	private String hashTag;
	private int frequency;
	private int deltaVal;

	
	public WordCounterBoltOutput(Date hashTagCreatedAt,String hashTag,int frequency,int deltaVal)
	{
		this.hashTag=hashTag;
		this.hashTagCreatedAt=hashTagCreatedAt;
		this.frequency= frequency;
		this.deltaVal=deltaVal;
		
	}
	public int getDeltaVal() {
		return deltaVal;
	}
	public void setDeltaVal(int deltaVal) {
		this.deltaVal = deltaVal;
	}
	public Date getHashTagCreatedAt() {
		return hashTagCreatedAt;
	}
	public void setHashTagCreatedAt(Date hashTagCreatedAt) {
		this.hashTagCreatedAt = hashTagCreatedAt;
	}
	public String getHashTag() {
		return hashTag;
	}
	public void setHashTag(String hashTag) {
		this.hashTag = hashTag;
	}
	public int getFrequency() {
		return frequency;
	}
	public void setFrequency(int frequency) {
		this.frequency = frequency;
	}
	

}
