import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;


public class SpoutOutputFormat implements Serializable {
	private Date hashTagCreatedAt;
	private String hashTag;
	private ArrayList<String> hashTags;
	
	public SpoutOutputFormat()
	{
		hashTagCreatedAt= new Date();
		hashTag="";
		hashTags= new ArrayList<String>();
	}
	public SpoutOutputFormat(Date hashTagCreatedAt,String hashTag)
	{
		this.hashTag=hashTag;
		this.hashTagCreatedAt=hashTagCreatedAt;
	}
	public SpoutOutputFormat(Date hashTagCreatedAt,String hashTag,ArrayList<String> hashTags)
	{
		this.hashTag=hashTag;
		this.hashTagCreatedAt=hashTagCreatedAt;
		this.hashTags=hashTags;
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
	
	
	public void setHashTags(ArrayList<String> hashTag)
	{
		this.hashTags.addAll(hashTag);
	}
	public ArrayList<String> getHashTags()
	{
		return this.hashTags;
	}
	public void setHashTag(String hashTag) {
		this.hashTag = hashTag;
	}
}
