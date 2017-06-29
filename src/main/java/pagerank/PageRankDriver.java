package pagerank;

import java.io.IOException;


/**
 * Created by xiaohei on 16/3/9.
 */
public class PageRankDriver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        // 生成概率矩阵
    	AdjacencyMatrix.run();
        for (int i = 0; i < 10; i++) {
        	// 2.迭代
            CalcPageRank.run();
        }
        // 标准化
        Standardization.run();
    }
}
