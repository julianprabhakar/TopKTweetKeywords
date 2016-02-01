package org.dic;

import java.util.Comparator;

/**
* 
* @author - Julian Prabhakar
* This is needed for the top-K list in descending order
*/
class TopKComparator implements Comparator<TopKElement>  {

	public int compare(TopKElement firstElement, TopKElement secondElement) {
		int retVal;
		try {
			retVal = (int)(secondElement.getCount() - firstElement.getCount());
		}catch(Exception e)
		{
			//Dont crash, continue with processing
			System.out.println("Exception occurred in top-k comparator--"+e);
			return 0;
		}
		return retVal;
	}
}