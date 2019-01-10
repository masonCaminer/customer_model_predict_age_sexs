package server.platform.wutiao;

/**
 * @program: customer_model_predict_age_sex
 * @description:
 * @author: maoyunlong
 * @create: 2018-11-24 14:29
 **/
public class t {
    public static void main(String[] args){
        int[]a = {5,4,3,2,1};
        int[]b = {5,4,3,2,1};
        test(a,0,a.length-1);
        for(int j:a){
            System.out.println(j);
        }
        t(b);
        for(int j:b){
            System.out.println(j);
        }
    }
    public static void test(int[]a,int low,int high){
        int start =low;
        int end = high;
        int key=a[low];
        if(end>start){
            while(end>start && a[end]>=key)
                end--;
            if(a[end]<=key){
                int tmp=a[end];
                a[end] = a[start];
                a[start]=tmp;
            }
            while(end>start&& a[start]<=key)
                start++;
            if(a[start]>=key){
                int tmp=a[start];
                a[start]=a[end];
                a[end]=tmp;
            }
            if(start>low)test(a,low,start-1);
            if(end<high)test(a,end+1,high);
        }
    }
    public static void t(int[]a){
        for(int i=0;i<a.length-1;i++)
            for(int j=0;j<a.length-1-i;j++)
                if(a[j]>a[j+1]){
                    int temp=a[j];
                    a[j]= a[j+1];
                    a[j+1]=temp;
                }
    }
}
