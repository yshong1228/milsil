// ── 밀실극장 Firebase Auth ──
// app.js 이후 로드됨. firebase.initializeApp() 완료 후 실행.

(function(){
  'use strict';

  // 로컬 파일 모드이거나 Firestore 초기화 실패 시 Auth 건너뜀
  if(window.USING_LOCAL_DB || window.IS_FILE_PROTOCOL){
    var ov=document.getElementById('loginOverlay');
    if(ov) ov.style.display='none';
    window.signInWithGoogle=function(){};
    window.doSignOut=function(){};
    return;
  }

  var auth;
  try{
    auth=firebase.auth();
  }catch(e){
    console.error('[Auth] 초기화 실패:', e);
    var ov2=document.getElementById('loginOverlay');
    if(ov2) ov2.style.display='none';
    return;
  }

  // ── Auth 상태 변경 감지 ──
  auth.onAuthStateChanged(function(user){
    window.currentUser=user;

    var overlay=document.getElementById('loginOverlay');
    var profile=document.getElementById('userProfile');
    var avatarImg=document.getElementById('userAvatarImg');
    var nameEl=document.getElementById('userNameText');

    if(user){
      // 로그인됨
      if(overlay) overlay.style.display='none';
      if(profile) profile.style.display='flex';
      if(avatarImg){
        avatarImg.src=user.photoURL||'';
        avatarImg.style.display=user.photoURL?'block':'none';
      }
      if(nameEl){
        var displayName=user.displayName||user.email||'';
        // 이름이 길면 앞부분만 표시
        nameEl.textContent=displayName.length>8?displayName.substring(0,8)+'…':displayName;
        nameEl.title=displayName;
      }
    }else{
      // 로그아웃됨
      if(overlay) overlay.style.display='flex';
      if(profile) profile.style.display='none';
    }
  });

  // ── Google 로그인 ──
  window.signInWithGoogle=function(){
    var provider=new firebase.auth.GoogleAuthProvider();
    // 한국어 UI
    provider.setCustomParameters({hl:'ko'});

    auth.signInWithPopup(provider).catch(function(err){
      console.error('[Auth] Google 로그인 실패:', err);
      var msg;
      switch(err.code){
        case 'auth/popup-closed-by-user':
          msg='로그인이 취소되었습니다';break;
        case 'auth/popup-blocked':
          msg='팝업이 차단되었습니다. 팝업 허용 후 다시 시도해주세요';break;
        case 'auth/network-request-failed':
          msg='네트워크 오류가 발생했습니다';break;
        default:
          msg='로그인에 실패했습니다';
      }
      if(typeof showToast==='function') showToast(msg);
    });
  };

  // ── 로그아웃 ──
  window.doSignOut=function(){
    auth.signOut().then(function(){
      if(typeof showToast==='function') showToast('로그아웃 되었습니다');
    }).catch(function(err){
      console.error('[Auth] 로그아웃 실패:', err);
    });
  };

})();
