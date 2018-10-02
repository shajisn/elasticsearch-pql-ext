package org.elasticsearch.pql.utils;

public class Condition {
	String leftPart = "";
	String operatorPart = "";
	String rightPart = "";
	
	public Condition(String leftPart, String operatorPart, String rightPart) {
		super();
		this.leftPart = leftPart;
		this.operatorPart = operatorPart;
		this.rightPart = rightPart;
	}

	public String getLeftPart() {
		return leftPart;
	}

	public void setLeftPart(String leftPart) {
		this.leftPart = leftPart;
	}

	public String getOperatorPart() {
		return operatorPart;
	}

	public void setOperatorPart(String operatorPart) {
		this.operatorPart = operatorPart;
	}

	public String getRightPart() {
		return rightPart;
	}

	public void setRightPart(String rightPart) {
		this.rightPart = rightPart;
	}

	@Override
	public String toString() {
		return "Condition [" + leftPart +  operatorPart +  rightPart + "]";
	}
	
}
