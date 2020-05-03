import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zhaozhuo
 * @date 2019/9/6
 */
public class Test  implements Serializable {
    private static final long serialVersionUID=1L;

    public static void main(String[] args) {
        Map<String,Object> map=new HashMap<>();
        System.out.println(map.size());
    }
}
