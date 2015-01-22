package nl.utwente.bigdata;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.io.IOException;

import java.io.BufferedReader;
import java.util.*;
import java.io.FileReader;
import java.io.File;

public class Controller {
	static List<String> players = new ArrayList<String>(); 
	static List<String> posWords = new ArrayList<String>();
	static List<String> negWords = new ArrayList<String>();
	public ArrayList<String> words= new ArrayList<String>();
	
	
	public Controller(){
		try{
			BufferedReader negReader = new BufferedReader(new FileReader(new File(
					"/home/s1346466/badWords.txt")));
			BufferedReader posReader = new BufferedReader(new FileReader(new File(
					"/home/s1346466/goodWords.txt")));
			BufferedReader playerReader = new BufferedReader(new FileReader(new File(
					"/home/s1346466/players.txt")));
					
			String word;

			// add words to comparison list
			while ((word = negReader.readLine()) != null) {
				negWords.add(word);
			}
			while ((word = posReader.readLine()) != null) {
				posWords.add(word);
			}
			while ((word = playerReader.readLine()) != null) {
				players.add(word);
			}
			System.err.println("Lengte van playerlijst" + players.size());
			System.err.println("Lengte van negativewords" + negWords.size());
			System.err.println("Lengte van positivewords" + posWords.size());
			// cleanup
			negReader.close();
			posReader.close();
			playerReader.close();
		}catch(IOException e){
           System.err.println("Failure when reading the text files");
		}
		
	}
	
	public Integer getMood(String tweet){
		String[] words = tweet.split(" ");
		int posCounter=0;
		int negCounter=0;
		// check if the current word appears in our reference lists...
		for (int i = 0; i < words.length; i++) {
			if (posWords.contains(words[i])) {
				posCounter++;
			}
			if (negWords.contains(words[i])) {
				negCounter++;
			}
		}
		return  (posCounter - negCounter);
	}
	
	public String hasPlayer(String tweet){
		String result="";
		for (int i = 0; i < players.size(); i++) {
			if(tweet.contains(players.get(i))) {
				result=players.get(i);
				return result;
			}
		}
		return result;
	}
}