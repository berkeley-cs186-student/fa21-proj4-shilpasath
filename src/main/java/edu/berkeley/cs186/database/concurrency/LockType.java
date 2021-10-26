package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.common.Pair;

import java.lang.reflect.Array;
import java.util.*;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }


        HashMap<Set<LockType>, Boolean> list = new HashMap<>();

        list.put(new HashSet<>(Arrays.asList(NL, NL)), true);

        list.put(new HashSet<>(Arrays.asList(IS, IS)), true);
        list.put(new HashSet<>(Arrays.asList(IS, IX)), true);
        list.put(new HashSet<>(Arrays.asList(IS, S)), true);
        list.put(new HashSet<>(Arrays.asList(IS, SIX)), true);
        list.put(new HashSet<>(Arrays.asList(IS, X)), false);
        list.put(new HashSet<>(Arrays.asList(IS, NL)), true);


        list.put(new HashSet<>(Arrays.asList(IX, IS)), true);
        list.put(new HashSet<>(Arrays.asList(IX, IX)), true);
        list.put(new HashSet<>(Arrays.asList(IX, S)), false);
        list.put(new HashSet<>(Arrays.asList(IX, SIX)), false);
        list.put(new HashSet<>(Arrays.asList(IX, X)), false);
        list.put(new HashSet<>(Arrays.asList(IX, NL)), true);

        list.put(new HashSet<>(Arrays.asList(S, IS)), true);
        list.put(new HashSet<>(Arrays.asList(S, IX)), false);
        list.put(new HashSet<>(Arrays.asList(S, S)), true);
        list.put(new HashSet<>(Arrays.asList(S, SIX)), false);
        list.put(new HashSet<>(Arrays.asList(S, X)), false);
        list.put(new HashSet<>(Arrays.asList(S, NL)), true);

        list.put(new HashSet<>(Arrays.asList(SIX, IS)), true);
        list.put(new HashSet<>(Arrays.asList(SIX, IX)), false);
        list.put(new HashSet<>(Arrays.asList(SIX, S)), false);
        list.put(new HashSet<>(Arrays.asList(SIX, SIX)), false);
        list.put(new HashSet<>(Arrays.asList(SIX, X)), false);
        list.put(new HashSet<>(Arrays.asList(SIX, NL)), true);

        list.put(new HashSet<>(Arrays.asList(X, IS)), false);
        list.put(new HashSet<>(Arrays.asList(X, IX)), false);
        list.put(new HashSet<>(Arrays.asList(X, S)), false);
        list.put(new HashSet<>(Arrays.asList(X, SIX)), false);
        list.put(new HashSet<>(Arrays.asList(X, X)), false);
        list.put(new HashSet<>(Arrays.asList(X, NL)), true);

        HashSet ab = new HashSet<>(Arrays.asList(a,b));
        boolean ret = list.get(ab);
        return ret;

        // TODO(proj4_part1): implement
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }

        if (childLockType.equals(NL)) {
            return true;
        } else if (parentLockType.equals(NL) || parentLockType.equals(S) || parentLockType.equals(X)) {
            return false;
        } else if (parentLockType.equals(IX)) {
            return true;
        }

        HashMap<List<LockType>, Boolean> list = new HashMap<>();

        list.put(new ArrayList<>(Arrays.asList(IS, IS)), true);
        list.put(new ArrayList<>(Arrays.asList(IS, IX)), false);
        list.put(new ArrayList<>(Arrays.asList(IS, S)), true);
        list.put(new ArrayList<>(Arrays.asList(IS, SIX)), false);
        list.put(new ArrayList<>(Arrays.asList(IS, X)), false);
        list.put(new ArrayList<>(Arrays.asList(IS, NL)), true);


        list.put(new ArrayList<>(Arrays.asList(SIX, IS)), false);
        list.put(new ArrayList<>(Arrays.asList(SIX, IX)), true);
        list.put(new ArrayList<>(Arrays.asList(SIX, S)), false);
        list.put(new ArrayList<>(Arrays.asList(SIX, SIX)), false);
        list.put(new ArrayList<>(Arrays.asList(SIX, X)), true);
        list.put(new ArrayList<>(Arrays.asList(IX, NL)), true);


        List new_list = new ArrayList<>(Arrays.asList(parentLockType, childLockType));
        boolean ret = list.get(new_list);
        return ret;

        // TODO(proj4_part1): implement
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }

        if (required.equals(NL)) {
            return true;
        } else if (substitute.equals(NL)) {
            return false;
        }

        HashMap<List<LockType>, Boolean> list = new HashMap<>();

        list.put(new ArrayList<>(Arrays.asList(IS, IS)), true);
        list.put(new ArrayList<>(Arrays.asList(IS, IX)), false);
        list.put(new ArrayList<>(Arrays.asList(IS, S)), false);
        list.put(new ArrayList<>(Arrays.asList(IS, SIX)), false);
        list.put(new ArrayList<>(Arrays.asList(IS, X)), false);

        list.put(new ArrayList<>(Arrays.asList(IX, IS)), true);
        list.put(new ArrayList<>(Arrays.asList(IX, IX)), true);
        list.put(new ArrayList<>(Arrays.asList(IX, S)), false);
        list.put(new ArrayList<>(Arrays.asList(IX, SIX)), false);
        list.put(new ArrayList<>(Arrays.asList(IX, X)), false);

        list.put(new ArrayList<>(Arrays.asList(S, IS)), false);
        list.put(new ArrayList<>(Arrays.asList(S, IX)), false);
        list.put(new ArrayList<>(Arrays.asList(S, S)), true);
        list.put(new ArrayList<>(Arrays.asList(S, SIX)), false);
        list.put(new ArrayList<>(Arrays.asList(S, X)), false);

        list.put(new ArrayList<>(Arrays.asList(SIX, IS)), false);
        list.put(new ArrayList<>(Arrays.asList(SIX, IX)), false);
        list.put(new ArrayList<>(Arrays.asList(SIX, S)), true);
        list.put(new ArrayList<>(Arrays.asList(SIX, SIX)), true);
        list.put(new ArrayList<>(Arrays.asList(SIX, X)), false);

        list.put(new ArrayList<>(Arrays.asList(X, IS)), false);
        list.put(new ArrayList<>(Arrays.asList(X, IX)), false);
        list.put(new ArrayList<>(Arrays.asList(X, S)), true);
        list.put(new ArrayList<>(Arrays.asList(X, SIX)), false);
        list.put(new ArrayList<>(Arrays.asList(X, X)), true);

        List new_list = new ArrayList<>(Arrays.asList(substitute, required));
        boolean ret = list.get(new_list);
        return ret;
        // TODO(proj4_part1): implement

    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

