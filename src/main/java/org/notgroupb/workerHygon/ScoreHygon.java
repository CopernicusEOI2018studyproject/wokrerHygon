package org.notgroupb.workerHygon;

public final class ScoreHygon {
	//MHW: means highest  water level,
	//MW: Average of water levels in a time span,
	//MNW: mean lowest value of water levels in a period of time
	private double MHW,MW,MNW;
	private double slopeMH,slopeLH;

	public void initScore(double b, double c, double d)
	{
		MHW = b;
		MW = c;
		MNW = d;
	}
	public double score(double currentWL)
	{
		double midMH = (MHW+MW)/2; //(4-9}
		slopeMH = 4/midMH;
		double midLM = (MW+MNW)/2; //(0-4}
		slopeLH = 5/midLM;
		//set different slope
		double score=0.0;
		
		if (currentWL < midLM)
		{
			score = 0;
		}
		if(currentWL > midLM & currentWL <= midMH)
		{
			score = slopeLH*currentWL;
		}
		if(currentWL > midMH & currentWL < MHW)
		{
			score = slopeMH*(currentWL-midMH)+5;
		}
		if (currentWL > MHW)
		{
			score = 10;
		}
		return score*10;
	}
	
	
}