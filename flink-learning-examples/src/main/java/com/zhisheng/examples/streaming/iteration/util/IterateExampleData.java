package com.zhisheng.examples.streaming.iteration.util;

/**
 * Data for IterateExampleITCase.
 */
public class IterateExampleData {
	public static final String INPUT_PAIRS = "(1,40)\n" + "(29,38)\n" + "(11,15)\n" + "(17,39)\n" + "(24,41)\n" +
			"(7,33)\n" + "(20,2)\n" + "(11,5)\n" + "(3,16)\n" + "(23,36)\n" + "(15,23)\n" + "(28,13)\n" + "(1,1)\n" +
			"(10,6)\n" + "(21,5)\n" + "(14,36)\n" + "(17,15)\n" + "(7,9)";

	public static final String RESULTS = "((1,40),3)\n" + "((24,41),2)\n" + "((3,16),5)\n" + "((1,1),10)\n" +
			"((17,15),4)\n" + "((29,38),2)\n" + "((7,33),3)\n" + "((23,36),3)\n" + "((10,6),6)\n" + "((7,9),5)\n" +
			"((11,15),4)\n" + "((20,2),5)\n" + "((15,23),4)\n" + "((21,5),5)\n" +
			"((17,39),3)\n" + "((11,5),6)\n" + "((28,13),4)\n" + "((14,36),3)";

	private IterateExampleData() {
	}
}
