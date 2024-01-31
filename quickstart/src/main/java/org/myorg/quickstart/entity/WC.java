package org.myorg.quickstart.entity;

public class WC {

    private String word;

    private Integer cnt;

    public WC() {
    }

    public WC(String word, Integer cnt) {
        this.word = word;
        this.cnt = cnt;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getCnt() {
        return cnt;
    }

    public void setCnt(Integer cnt) {
        this.cnt = cnt;
    }

    @Override
    public String toString() {
        return "WC{" +
                "word='" + word + '\'' +
                ", cnt=" + cnt +
                '}';
    }
}
