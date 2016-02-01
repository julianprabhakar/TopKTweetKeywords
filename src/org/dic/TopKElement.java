package org.dic;

/*
* @author: Julian
* This is the POJO that represents a topKElement
*/
class TopKElement {

	private String element;
	private Long count;

	public TopKElement(String ele, Long cnt)
	{
		element = ele; 
		count = cnt;
	}
	public TopKElement(String ele)
	{
		element = ele; 
		count = 0l;
	}
	public TopKElement()
	{
	}

	public Long getCount()
	{
		return count;
	}

	public void setCount(Long l)
	{
		count = l;
	}

	public String getElement()
	{
		return element;
	}

	public void setElement(String s)
	{
		element = s;
	}

	// Notice we are comparing only the element name and not the count
	@Override
    public boolean equals(Object obj) {
 
        if (obj == this) {
            return true;
        }
 
        if (!(obj instanceof TopKElement)) {
            return false;
        }
         
        TopKElement objTopKEle = (TopKElement) obj;
         
        return element.equals(objTopKEle.getElement()) ;
    }
	//For printing a topKElement
	public String toString()
	{
		return " "+element + ":" + count+" ";
	}

}